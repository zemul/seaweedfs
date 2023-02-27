package command

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/syndtr/goleveldb/leveldb"
	leveldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	leveldb_util "github.com/syndtr/goleveldb/leveldb/util"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type FilerIncrBackupOptions struct {
	isActivePassive *bool
	filer           *string
	path            *string
	excludePaths    *string
	debug           *bool
	proxyByFiler    *bool
	timeAgo         *time.Duration
	retentionDays   *int
	metaFolder      *string
}

var (
	filerIncrBackupOptions FilerIncrBackupOptions
	db                     *leveldb.DB
)

func init() {
	cmdFilerIncrBackup.Run = runFilerIncrBackup // break init cycle
	filerIncrBackupOptions.filer = cmdFilerIncrBackup.Flag.String("filer", "localhost:8888", "filer of one SeaweedFS cluster")
	filerIncrBackupOptions.path = cmdFilerIncrBackup.Flag.String("filerPath", "/", "directory to sync on filer")
	filerIncrBackupOptions.excludePaths = cmdFilerIncrBackup.Flag.String("filerExcludePaths", "", "exclude directories to sync on filer")
	filerIncrBackupOptions.proxyByFiler = cmdFilerIncrBackup.Flag.Bool("filerProxy", false, "read and write file chunks by filer instead of volume servers")
	filerIncrBackupOptions.debug = cmdFilerIncrBackup.Flag.Bool("debug", false, "debug mode to print out received files")
	filerIncrBackupOptions.timeAgo = cmdFilerIncrBackup.Flag.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	filerIncrBackupOptions.retentionDays = cmdFilerIncrBackup.Flag.Int("retentionDays", 0, "incremental backup retention days")
	filerIncrBackupOptions.metaFolder = cmdFilerIncrBackup.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
}

var cmdFilerIncrBackup = &Command{
	UsageLine: "filer.incr.backup -filer=<filerHost>:<filerPort> ",
	Short:     "resume-able continuously replicate files from a SeaweedFS cluster to another location defined in replication.toml",
	Long: `resume-able continuously replicate files from a SeaweedFS cluster to another location defined in replication.toml

	filer.backup listens on filer notifications. If any file is updated, it will fetch the updated content,
	and write to the destination. This is to replace filer.replicate command since additional message queue is not needed.

	If restarted and "-timeAgo" is not set, the synchronization will resume from the previous checkpoints, persisted every minute.
	A fresh sync will start from the earliest metadata logs. To reset the checkpoints, just set "-timeAgo" to a high value.

`,
}

func runFilerIncrBackup(cmd *Command, args []string) bool {
	util.LoadConfiguration("security", false)
	util.LoadConfiguration("replication", true)

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	clientId := util.RandomInt32()
	var clientEpoch int32

	for {
		clientEpoch++
		err := doFilerIncrBackup(grpcDialOption, &filerIncrBackupOptions, clientId, clientEpoch)
		if err != nil {
			glog.Errorf("backup from %s: %v", *filerIncrBackupOptions.filer, err)
			time.Sleep(1747 * time.Millisecond)
		}
	}

	return true
}

const (
	IncrBackupKeyPrefix = "backup.incr"
)

func doFilerIncrBackup(grpcDialOption grpc.DialOption, backupOption *FilerIncrBackupOptions, clientId int32, clientEpoch int32) error {

	// find data sink
	config := util.GetViper()
	dataSink := findSink(config)
	if dataSink == nil {
		return fmt.Errorf("no data sink configured in replication.toml")
	}

	// init meta log store
	opts := &opt.Options{
		BlockCacheCapacity: 32 * 1024 * 1024,         // default value is 8MiB
		WriteBuffer:        16 * 1024 * 1024,         // default value is 4MiB
		Filter:             filter.NewBloomFilter(8), // false positive rate 0.02
	}
	var err error
	if db, err = leveldb.OpenFile(*filerIncrBackupOptions.metaFolder, opts); err != nil {
		if leveldb_errors.IsCorrupted(err) {
			db, err = leveldb.RecoverFile(*filerIncrBackupOptions.metaFolder, opts)
		}
		if err != nil {
			return fmt.Errorf("filer store open dir %s: %v", *filerIncrBackupOptions.metaFolder, err)
		}
	}
	grace.OnInterrupt(func() {
		db.Close()
	})

	if *filerIncrBackupOptions.debug {
		iter := db.NewIterator(&leveldb_util.Range{}, nil)
		for iter.Next() {
			glog.V(0).Infof("key:%d", util.BytesToUint64(iter.Key()))
		}
		iter.Release()
	}

	sourceFiler := pb.ServerAddress(*backupOption.filer)
	sourcePath := *backupOption.path
	excludePaths := util.StringSplit(*backupOption.excludePaths, ",")
	timeAgo := *backupOption.timeAgo
	targetPath := dataSink.GetSinkToDirectory()
	debug := *backupOption.debug

	// get start time for the data sink
	startFrom := time.Now().Local()
	sinkId := util.HashStringToLong(dataSink.GetName() + dataSink.GetSinkToDirectory())
	if timeAgo.Milliseconds() == 0 {
		lastOffsetTsNs, err := getOffset(grpcDialOption, sourceFiler, IncrBackupKeyPrefix, int32(sinkId))
		if err != nil {
			glog.V(0).Infof("starting from %v", startFrom)
		} else {
			startFrom = time.Unix(0, lastOffsetTsNs)
			glog.V(0).Infof("resuming from %v", startFrom)
		}
	} else {
		startFrom = startFrom.Add(-timeAgo)
		glog.V(0).Infof("start time is set to %v", startFrom)
	}

	// create filer sink
	filerSource := &source.FilerSource{}
	filerSource.DoInitialize(
		sourceFiler.ToHttpAddress(),
		sourceFiler.ToGrpcAddress(),
		sourcePath,
		*backupOption.proxyByFiler)
	dataSink.SetSourceFiler(filerSource)

	processEventFn := genIncrProcessFunction(sourcePath, targetPath, excludePaths, dataSink, debug)

	processEventFnWithOffset := pb.AddOffsetFunc(processEventFn, 3*time.Second, func(counter int64, lastTsNs int64) error {
		glog.V(0).Infof("backup %s progressed to %v %0.2f/sec", sourceFiler, time.Unix(0, lastTsNs), float64(counter)/float64(3))
		return setOffset(grpcDialOption, sourceFiler, IncrBackupKeyPrefix, int32(sinkId), lastTsNs)
	})

	if *filerIncrBackupOptions.retentionDays > 0 {
		go func() {
			for {
				now := time.Now().Local()
				time.Sleep(time.Hour * 24)
				key := now.Add(-1 * time.Hour * 24 * time.Duration(*filerIncrBackupOptions.retentionDays)).Format("2006-01-02")
				_ = dataSink.DeleteEntry(util.Join(targetPath, key), true, true, nil)
				glog.V(0).Infof("incremental backup delete directory:%s", key)
				validTs := now.Add(-1 * time.Hour * 24 * time.Duration(*filerIncrBackupOptions.retentionDays)).UnixNano()
				iter := db.NewIterator(&leveldb_util.Range{}, nil)
				if iter.Next() {
					ts, err := strconv.ParseInt(string(iter.Value()), 10, 64)
					if err != nil {
						glog.V(0).Infof("parse value error , v= %s, err= %v", v, err)
						continue
					}
					if ts < validTs {
						_ = db.Delete(iter.Key(), nil)
					} else {
						break
					}
				}
				iter.Release()
			}
		}()
	}

	return pb.FollowMetadata(sourceFiler, grpcDialOption, "backup_"+dataSink.GetName(), clientId, clientEpoch, sourcePath, nil, startFrom.UnixNano(), 0, 0, processEventFnWithOffset, pb.TrivialOnError)
}

func genIncrProcessFunction(sourcePath string, targetPath string, excludePaths []string, dataSink sink.ReplicationSink, debug bool) func(resp *filer_pb.SubscribeMetadataResponse) error {
	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification
		tsNs := resp.TsNs

		var sourceOldKey, sourceNewKey util.FullPath
		if message.OldEntry != nil {
			sourceOldKey = util.FullPath(resp.Directory).Child(message.OldEntry.Name)
		}
		if message.NewEntry != nil {
			sourceNewKey = util.FullPath(message.NewParentPath).Child(message.NewEntry.Name)
		}

		if debug {
			glog.V(0).Infof("received %v", resp)
		}

		if !strings.HasPrefix(resp.Directory, sourcePath) {
			return nil
		}
		for _, excludePath := range excludePaths {
			if strings.HasPrefix(resp.Directory, excludePath) {
				return nil
			}
		}

		// save event
		event, err := proto.Marshal(resp)
		if err != nil {
			glog.Errorf("failed to marshal filer_pb.SubscribeMetadataResponse %+v: %v", resp, err)
			return nil
		}
		tsBuf := make([]byte, 8)
		util.Uint64toBytes(tsBuf, uint64(tsNs))
		if err = db.Put(tsBuf, event, nil); err != nil {
			return err
		}

		// handle deletions
		if filer_pb.IsDelete(resp) {
			return nil
		}

		// handle new entries
		if filer_pb.IsCreate(resp) {
			if !strings.HasPrefix(string(sourceNewKey), sourcePath) {
				return nil
			}
			_, bp := buildIncrKey(tsNs, message, targetPath, sourceNewKey, sourcePath)
			return dataSink.CreateEntry(bp, message.NewEntry, message.Signatures)
		}

		// this is something special?
		if filer_pb.IsEmpty(resp) {
			return nil
		}

		// handle updates
		if strings.HasPrefix(string(sourceOldKey), sourcePath) {
			// old key is in the watched directory
			if strings.HasPrefix(string(sourceNewKey), sourcePath) {
				// new key is also in the watched directory
				_, bp := buildIncrKey(tsNs, message, targetPath, sourceNewKey, sourcePath)
				return dataSink.CreateEntry(bp, message.NewEntry, message.Signatures)
			} else {
				// new key is outside of the watched directory
				return nil
			}
		} else {
			// old key is outside of the watched directory
			if strings.HasPrefix(string(sourceNewKey), sourcePath) {
				// new key is in the watched directory
				_, bp := buildIncrKey(tsNs, message, targetPath, sourceNewKey, sourcePath)
				return dataSink.CreateEntry(filepath.Join(targetPath, bp), message.NewEntry, message.Signatures)
			} else {
				// new key is also outside of the watched directory
				// skip
			}
		}

		return nil

	}
	return processEventFn
}

func buildIncrKey(tsNs int64, message *filer_pb.EventNotification, targetPath string, sourceKey util.FullPath, sourcePath string) (ns string, bp string) {
	var mTime int64
	if message.NewEntry != nil {
		mTime = message.NewEntry.Attributes.Mtime
	} else if message.OldEntry != nil {
		mTime = message.OldEntry.Attributes.Mtime
	}
	dateKey := time.Unix(mTime, 0).Format("2006-01-02")
	ns = fmt.Sprintf("%d", tsNs)
	bp = util.Join(targetPath, dateKey, string(sourceKey)[len(sourcePath):]+fmt.Sprintf("_%d", mTime))
	return
}
