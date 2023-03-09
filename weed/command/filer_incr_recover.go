package command

import (
	"context"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FilerIncrRecoverOptions struct {
	filer            *string
	path             *string
	debug            *bool
	retentionDays    *int
	metaFolder       *string
	start            *string
	end              *string
	concurrentChunks *int

	maxMB       *int
	checkSize   *bool
	verbose     *bool
	replication *string
	collection  *string
	ttl         *string
	diskType    *string
	include     *string
}

var (
	filerIncrRecoverOptions FilerIncrRecoverOptions
)

const (
	NormTimeFmt = "2006-01-02 15:04:05"
)

func init() {
	cmdFilerIncrRecover.Run = runFilerIncrRecover // break init cycle
	filerIncrRecoverOptions.filer = cmdFilerIncrRecover.Flag.String("filer", "localhost:8888", "filer of one SeaweedFS cluster")
	filerIncrRecoverOptions.path = cmdFilerIncrRecover.Flag.String("filerPath", "/buckets/example", "directory to sync on filer")
	filerIncrRecoverOptions.debug = cmdFilerIncrRecover.Flag.Bool("debug", false, "debug mode to print out received files")
	filerIncrRecoverOptions.metaFolder = cmdFilerIncrRecover.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	filerIncrRecoverOptions.start = cmdFilerIncrRecover.Flag.String("start", "2020-01-01 00:00:00", "start time")
	filerIncrRecoverOptions.end = cmdFilerIncrRecover.Flag.String("end", time.Now().Format(NormTimeFmt), "end time")
	filerIncrRecoverOptions.verbose = cmdFilerIncrRecover.Flag.Bool("verbose", false, "print out details during copying")
	filerIncrRecoverOptions.concurrentChunks = cmdFilerIncrRecover.Flag.Int("concurrentChunks", 8, "concurrent chunk copy goroutines for each file")

	filerIncrRecoverOptions.maxMB = cmdFilerIncrRecover.Flag.Int("maxMB", 4, "split files larger than the limit")
	filerIncrRecoverOptions.checkSize = cmdFilerIncrRecover.Flag.Bool("check.size", false, "copy when the target file size is different from the source file")
	filerIncrRecoverOptions.replication = cmdFilerIncrRecover.Flag.String("replication", "", "replication type")
	filerIncrRecoverOptions.collection = cmdFilerIncrRecover.Flag.String("collection", "", "optional collection name")
	filerIncrRecoverOptions.ttl = cmdFilerIncrRecover.Flag.String("ttl", "", "time to live, e.g.: 1m, 1h, 1d, 1M, 1y")
	filerIncrRecoverOptions.diskType = cmdFilerIncrRecover.Flag.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	filerIncrRecoverOptions.include = cmdFilerIncrRecover.Flag.String("include", "", "pattens of files to copy, e.g., *.pdf, *.html, ab?d.txt, works together with -dir")
}

var cmdFilerIncrRecover = &Command{
	UsageLine: "filer.incr.recover -filer=<filerHost>:<filerPort> ",
	Short:     "resume-able continuously replicate files from a SeaweedFS cluster to another location defined in replication.toml",
	Long: `resume-able continuously replicate files from a SeaweedFS cluster to another location defined in replication.toml

	filer.incr.backup listens on filer notifications. If any file is updated, it will fetch the updated content,
	and write to the destination. This is to replace filer.replicate command since additional message queue is not needed.

	note:
    Only restore "filer.incr.backup".
`,
}

func runFilerIncrRecover(cmd *Command, args []string) bool {
	util.LoadConfiguration("security", false)
	util.LoadConfiguration("replication", true)

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	err := doFilerIncrRecover(grpcDialOption, &filerIncrRecoverOptions)
	if err != nil {
		glog.Errorf("doFilerIncrRecover from %s: %v", *filerIncrRecoverOptions.filer, err)
	}

	return true
}

func doFilerIncrRecover(grpcDialOption grpc.DialOption, backupOption *FilerIncrRecoverOptions) (err error) {
	// find data sink
	config := util.GetViper()
	dataSink := findSink(config)
	if dataSink == nil {
		return fmt.Errorf("no data sink configured in replication.toml")
	}
	backupPath := dataSink.GetSinkToDirectory()

	// create remote sink
	targetFiler := pb.ServerAddress(*backupOption.filer)

	// init meta log store
	opts := &opt.Options{
		BlockCacheCapacity: 32 * 1024 * 1024,         // default value is 8MiB
		WriteBuffer:        16 * 1024 * 1024,         // default value is 4MiB
		Filter:             filter.NewBloomFilter(8), // false positive rate 0.02
	}

	var db *leveldb.DB
	if db, err = leveldb.OpenFile(*backupOption.metaFolder, opts); err != nil {
		if leveldb_errors.IsCorrupted(err) {
			db, err = leveldb.RecoverFile(*backupOption.metaFolder, opts)
		}
		if err != nil {
			return fmt.Errorf("filer store open dir %s: %v", *backupOption.metaFolder, err)
		}
	}
	defer db.Close()
	grace.OnInterrupt(func() {
		_ = db.Close()
	})

	debug := *backupOption.debug
	start, err := time.ParseInLocation(NormTimeFmt, *backupOption.start, time.Local)
	if err != nil {
		return fmt.Errorf("parse time err:%v", err)
	}
	end, err := time.ParseInLocation(NormTimeFmt, *backupOption.end, time.Local)
	if err != nil {
		return fmt.Errorf("parse time err:%v", err)
	}
	fmt.Printf("recover start: %s, end:%s\n", *backupOption.start, *backupOption.end)

	// genTask
	eventChan := make(chan *filer_pb.SubscribeMetadataResponse, 20)
	go func() {
		defer close(eventChan)
		startTsBuf := make([]byte, 8)
		util.Uint64toBytes(startTsBuf, uint64(start.UnixNano()))
		iter := db.NewIterator(&leveldb_util.Range{Start: startTsBuf}, nil)
		for iter.Next() {
			ts := util.BytesToUint64(iter.Key())
			if ts > uint64(end.UnixNano()) {
				break
			}
			event := &filer_pb.SubscribeMetadataResponse{}
			err = proto.Unmarshal(iter.Value(), event)
			if err != nil {
				glog.V(0).Infof("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %v", err)
				break
			}
			eventChan <- event
		}
		iter.Release()

	}()

	worker := FileOperationWorker{
		options: &CopyOptions{
			include:          filerIncrRecoverOptions.include,
			replication:      filerIncrRecoverOptions.replication,
			collection:       filerIncrRecoverOptions.collection,
			ttl:              filerIncrRecoverOptions.ttl,
			diskType:         filerIncrRecoverOptions.diskType,
			maxMB:            filerIncrRecoverOptions.maxMB,
			concurrentChunks: filerIncrRecoverOptions.concurrentChunks,
			grpcDialOption:   grpcDialOption,
			checkSize:        filerIncrRecoverOptions.checkSize,
			verbose:          filerIncrRecoverOptions.verbose,
		},
		filerAddress: targetFiler,
		signature:    util.RandomInt32(),
	}

	processEventFn := genIncrRecoverProcessFunction(backupPath, *filerIncrRecoverOptions.path, []string{}, worker, debug)
	processEventFnWithOffset := pb.AddOffsetFunc(processEventFn, 3*time.Second, func(counter int64, lastTsNs int64) error {
		glog.V(0).Infof("recover %s progressed to %v %0.2f/sec", *filerIncrRecoverOptions.path, time.Unix(0, lastTsNs), float64(counter)/float64(3))
		return nil
	})
	// recover
	for event := range eventChan {
		if err := processEventFnWithOffset(event); err != nil {
			glog.Errorf("process %v: %v", event, err)
		}
	}
	return
}

type FileOperationWorker struct {
	options      *CopyOptions
	filerAddress pb.ServerAddress
	signature    int32
}

type FileOperationTask struct {
	sourceLocation     string
	destinationUrlPath string
	fileSize           int64
	fileMode           os.FileMode
	uid                uint32
	gid                uint32
	ctime              int64
	mtime              int64
}

func genIncrRecoverProcessFunction(sourcePath string, targetPath string, excludePaths []string, worker FileOperationWorker, debug bool) func(resp *filer_pb.SubscribeMetadataResponse) error {
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

		if !strings.HasPrefix(resp.Directory, targetPath) {
			return nil
		}
		for _, excludePath := range excludePaths {
			if strings.HasPrefix(resp.Directory, excludePath) {
				return nil
			}
		}

		if debug {
			glog.V(0).Infof("handle event: %v", resp)
		}

		// handle deletions
		if filer_pb.IsDelete(resp) {
			if !strings.HasPrefix(string(sourceOldKey), targetPath) {
				return nil
			}
			if debug {
				fmt.Printf("delete source:%s", string(sourceOldKey))
			}
			return worker.deleteEntry(string(sourceOldKey), []int32{})
		}

		// handle new entries
		if filer_pb.IsCreate(resp) {
			if !strings.HasPrefix(string(sourceNewKey), targetPath) {
				return nil
			}
			if debug {
				fmt.Printf("restore source:%s, desc:%s\n", buildRecvKey(tsNs, message, sourcePath, sourceNewKey), string(sourceNewKey))
			}
			return worker.doEachCopy(FileOperationTask{
				sourceLocation:     buildRecvKey(tsNs, message, sourcePath, sourceNewKey),
				destinationUrlPath: string(sourceNewKey),
			}, resp.EventNotification.NewEntry)
		}

		// this is something special?
		if filer_pb.IsEmpty(resp) {
			return nil
		}

		// handle updates
		if strings.HasPrefix(string(sourceOldKey), targetPath) {
			// old key is in the watched directory
			if strings.HasPrefix(string(sourceNewKey), targetPath) {
				// new key is also in the watched directory
				// delete the old entry
				if err := worker.deleteEntry(string(sourceOldKey), nil); err != nil {
					return err
				}
				// create the new entry
				return worker.doEachCopy(FileOperationTask{
					sourceLocation:     buildRecvKey(tsNs, message, sourcePath, sourceNewKey),
					destinationUrlPath: string(sourceNewKey),
				}, resp.EventNotification.NewEntry)
			} else {
				// new key is outside of the watched directory
				return worker.deleteEntry(string(sourceOldKey), nil)
			}
		} else {
			// old key is outside of the watched directory
			if strings.HasPrefix(string(sourceNewKey), targetPath) {
				// new key is in the watched directory
				return worker.doEachCopy(FileOperationTask{
					sourceLocation:     buildRecvKey(tsNs, message, sourcePath, sourceNewKey),
					destinationUrlPath: string(sourceNewKey),
				}, resp.EventNotification.NewEntry)
			} else {
				// new key is also outside of the watched directory
				// skip
			}
		}

		return nil
	}
	return processEventFn
}

func (worker *FileOperationWorker) doEachCopy(task FileOperationTask, entry *filer_pb.Entry) error {
	if entry.IsDirectory {
		return worker.createEntry(task.destinationUrlPath, entry, nil)
	}

	fi, err := os.Stat(task.sourceLocation)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: read file %s: %v\n", task.sourceLocation, err)
		return nil
	}
	task.uid, task.gid = entry.Attributes.Uid, entry.Attributes.Gid
	task.ctime, task.mtime = entry.Attributes.Crtime, entry.Attributes.Mtime
	task.fileSize = fi.Size()
	task.fileMode = fi.Mode()

	if task.fileMode.IsDir() {
		task.fileSize = 0
	}

	f, err := os.Open(task.sourceLocation)
	if err != nil {
		fmt.Printf("Failed to open file %s: %v\n", task.sourceLocation, err)
		if _, ok := err.(*os.PathError); ok {
			fmt.Printf("skipping %s\n", task.sourceLocation)
			return nil
		}
		return err
	}
	defer f.Close()

	// this is a regular file
	if *worker.options.include != "" {
		if ok, _ := filepath.Match(*worker.options.include, filepath.Base(task.sourceLocation)); !ok {
			return nil
		}
	}

	if shouldCopy, err := worker.checkExistingFileFirst(task, f); err != nil {
		return fmt.Errorf("check existing file: %v", err)
	} else if !shouldCopy {
		if *worker.options.verbose {
			fmt.Printf("skipping copied file: %v\n", f.Name())
		}
		return nil
	}

	// find the chunk count
	chunkSize := int64(*worker.options.maxMB * 1024 * 1024)
	chunkCount := 1
	if chunkSize > 0 && task.fileSize > chunkSize {
		chunkCount = int(task.fileSize/chunkSize) + 1
	}

	if chunkCount == 1 {
		return worker.uploadFileAsOne(task, f)
	}

	return worker.uploadFileInChunks(task, f, chunkCount, chunkSize)
}

func (worker *FileOperationWorker) checkExistingFileFirst(task FileOperationTask, f *os.File) (shouldCopy bool, err error) {

	shouldCopy = true

	if !*worker.options.checkSize {
		return
	}

	fileStat, err := f.Stat()
	if err != nil {
		shouldCopy = false
		return
	}

	err = pb.WithGrpcFilerClient(false, worker.signature, worker.filerAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		dir, name := util.FullPath(task.destinationUrlPath).DirAndName()
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		}

		resp, lookupErr := client.LookupDirectoryEntry(context.Background(), request)
		if lookupErr != nil {
			// mostly not found error
			return nil
		}

		if fileStat.Size() == int64(filer.FileSize(resp.Entry)) {
			shouldCopy = false
		}

		return nil
	})
	return
}

func (worker *FileOperationWorker) uploadFileAsOne(task FileOperationTask, f *os.File) error {

	// upload the file content
	dir, name := util.FullPath(task.destinationUrlPath).DirAndName()
	var mimeType string

	var chunks []*filer_pb.FileChunk

	if task.fileMode&os.ModeDir == 0 && task.fileSize > 0 {

		mimeType = detectMimeType(f)
		data, err := io.ReadAll(f)
		if err != nil {
			return err
		}

		finalFileId, uploadResult, flushErr, _ := operation.UploadWithRetry(
			worker,
			&filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: *worker.options.replication,
				Collection:  *worker.options.collection,
				TtlSec:      worker.options.ttlSec,
				DiskType:    *worker.options.diskType,
				Path:        dir,
			},
			&operation.UploadOption{
				Filename:          name,
				Cipher:            worker.options.cipher,
				IsInputCompressed: false,
				MimeType:          mimeType,
				PairMap:           nil,
			},
			func(host, fileId string) string {
				return fmt.Sprintf("http://%s/%s", host, fileId)
			},
			util.NewBytesReader(data),
		)
		if flushErr != nil {
			return flushErr
		}
		chunks = append(chunks, uploadResult.ToPbFileChunk(finalFileId, 0, time.Now().UnixNano()))
	}

	if err := pb.WithGrpcFilerClient(false, worker.signature, worker.filerAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name: name,
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   task.ctime,
					Mtime:    task.mtime,
					Gid:      task.gid,
					Uid:      task.uid,
					FileSize: uint64(task.fileSize),
					FileMode: uint32(task.fileMode),
					Mime:     mimeType,
					TtlSec:   worker.options.ttlSec,
				},
				Chunks: chunks,
			},
		}

		if err := filer_pb.CreateEntry(client, request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("upload data %v to http://%s%s%s: %v\n", name, worker.filerAddress.ToHttpAddress(), task.destinationUrlPath, name, err)
	}

	return nil
}

func (worker *FileOperationWorker) uploadFileInChunks(task FileOperationTask, f *os.File, chunkCount int, chunkSize int64) error {

	dir, name := util.FullPath(task.destinationUrlPath).DirAndName()
	mimeType := detectMimeType(f)

	chunksChan := make(chan *filer_pb.FileChunk, chunkCount)

	concurrentChunks := make(chan struct{}, *worker.options.concurrentChunks)
	var wg sync.WaitGroup
	var uploadError error

	fmt.Printf("uploading %s in %d chunks ...\n", name, chunkCount)
	for i := int64(0); i < int64(chunkCount) && uploadError == nil; i++ {
		wg.Add(1)
		concurrentChunks <- struct{}{}
		go func(i int64) {
			defer func() {
				wg.Done()
				<-concurrentChunks
			}()

			fileId, uploadResult, err, _ := operation.UploadWithRetry(
				worker,
				&filer_pb.AssignVolumeRequest{
					Count:       1,
					Replication: *worker.options.replication,
					Collection:  *worker.options.collection,
					TtlSec:      worker.options.ttlSec,
					DiskType:    *worker.options.diskType,
					Path:        task.destinationUrlPath,
				},
				&operation.UploadOption{
					Filename:          name + "-" + strconv.FormatInt(i+1, 10),
					Cipher:            worker.options.cipher,
					IsInputCompressed: false,
					MimeType:          "",
					PairMap:           nil,
				},
				func(host, fileId string) string {
					return fmt.Sprintf("http://%s/%s", host, fileId)
				},
				io.NewSectionReader(f, i*chunkSize, chunkSize),
			)

			if err != nil {
				uploadError = fmt.Errorf("upload data %v: %v\n", name, err)
				return
			}
			if uploadResult.Error != "" {
				uploadError = fmt.Errorf("upload %v result: %v\n", name, uploadResult.Error)
				return
			}
			chunksChan <- uploadResult.ToPbFileChunk(fileId, i*chunkSize, time.Now().UnixNano())

			fmt.Printf("uploaded %s-%d [%d,%d)\n", name, i+1, i*chunkSize, i*chunkSize+int64(uploadResult.Size))
		}(i)
	}
	wg.Wait()
	close(chunksChan)

	var chunks []*filer_pb.FileChunk
	for chunk := range chunksChan {
		chunks = append(chunks, chunk)
	}

	if uploadError != nil {
		var fileIds []string
		for _, chunk := range chunks {
			fileIds = append(fileIds, chunk.FileId)
		}
		operation.DeleteFiles(func() pb.ServerAddress {
			return pb.ServerAddress(copy.masters[0])
		}, false, worker.options.grpcDialOption, fileIds)
		return uploadError
	}

	manifestedChunks, manifestErr := filer.MaybeManifestize(worker.saveDataAsChunk, chunks)
	if manifestErr != nil {
		return fmt.Errorf("create manifest: %v", manifestErr)
	}

	if err := pb.WithGrpcFilerClient(false, worker.signature, worker.filerAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name: name,
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   task.ctime,
					Mtime:    task.mtime,
					Gid:      task.gid,
					Uid:      task.uid,
					FileSize: uint64(task.fileSize),
					FileMode: uint32(task.fileMode),
					Mime:     mimeType,
					TtlSec:   worker.options.ttlSec,
				},
				Chunks: manifestedChunks,
			},
		}

		if err := filer_pb.CreateEntry(client, request); err != nil {
			return fmt.Errorf("update fh: %v", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("upload data %v to http://%s%s%s: %v\n", name, worker.filerAddress.ToHttpAddress(), task.destinationUrlPath, name, err)
	}

	fmt.Printf("copied %s => http://%s%s%s\n", f.Name(), worker.filerAddress.ToHttpAddress(), task.destinationUrlPath, name)

	return nil
}

func (worker *FileOperationWorker) saveDataAsChunk(reader io.Reader, name string, offset int64, tsNs int64) (chunk *filer_pb.FileChunk, err error) {

	finalFileId, uploadResult, flushErr, _ := operation.UploadWithRetry(
		worker,
		&filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: *worker.options.replication,
			Collection:  *worker.options.collection,
			TtlSec:      worker.options.ttlSec,
			DiskType:    *worker.options.diskType,
			Path:        name,
		},
		&operation.UploadOption{
			Filename:          name,
			Cipher:            worker.options.cipher,
			IsInputCompressed: false,
			MimeType:          "",
			PairMap:           nil,
		},
		func(host, fileId string) string {
			return fmt.Sprintf("http://%s/%s", host, fileId)
		},
		reader,
	)

	if flushErr != nil {
		return nil, fmt.Errorf("upload data: %v", flushErr)
	}
	if uploadResult.Error != "" {
		return nil, fmt.Errorf("upload result: %v", uploadResult.Error)
	}
	return uploadResult.ToPbFileChunk(finalFileId, offset, tsNs), nil
}

var _ = filer_pb.FilerClient(&FileCopyWorker{})

func (worker *FileOperationWorker) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) (err error) {

	filerGrpcAddress := worker.filerAddress.ToGrpcAddress()
	err = pb.WithGrpcClient(streamingMode, worker.signature, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, filerGrpcAddress, false, worker.options.grpcDialOption, grpc.WithPerRPCCredentials(new(security.WithGrpcFilerTokenAuth)))
	return
}

func (worker *FileOperationWorker) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (worker *FileOperationWorker) GetDataCenter() string {
	return ""
}

func (worker *FileOperationWorker) deleteEntry(key string, signatures []int32) error {
	err := pb.WithGrpcFilerClient(false, worker.signature, worker.filerAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		dir, name := util.FullPath(key).DirAndName()
		req := &filer_pb.DeleteEntryRequest{
			Directory:            dir,
			Name:                 name,
			IgnoreRecursiveError: true,
			IsDeleteData:         true,
			IsRecursive:          true,
			Signatures:           signatures,
		}
		if resp, err := client.DeleteEntry(context.Background(), req); err != nil {
			if strings.Contains(err.Error(), "filer: no entry is found in filer store") {
				return nil
			}
			return err
		} else {
			if resp.Error != "" {
				if strings.Contains(resp.Error, "filer: no entry is found in filer store") {
					return nil
				}
				return errors.New(resp.Error)
			}
		}
		return nil
	})

	return err
}

func (worker *FileOperationWorker) createEntry(key string, entry *filer_pb.Entry, signatures []int32) error {
	return pb.WithGrpcFilerClient(false, worker.signature, worker.filerAddress, worker.options.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		dir, name := util.FullPath(key).DirAndName()
		request := &filer_pb.CreateEntryRequest{
			Directory:  dir,
			Entry:      entry,
			Signatures: signatures,
		}

		glog.V(1).Infof("mkdir: %v", request)
		if err := filer_pb.CreateEntry(client, request); err != nil {
			return fmt.Errorf("mkdir %s/%s: %v", dir, name, err)
		}

		return nil
	})
}

func buildRecvKey(tsNs int64, message *filer_pb.EventNotification, sourcePath string, sourceKey util.FullPath) (bp string) {
	var mTime int64
	if message.NewEntry != nil {
		mTime = message.NewEntry.Attributes.Mtime
	} else if message.OldEntry != nil {
		mTime = message.OldEntry.Attributes.Mtime
	}
	dateKey := time.Unix(mTime, 0).Format("2006-01-02")
	bp = util.Join(sourcePath, dateKey, string(sourceKey)+fmt.Sprintf("_%d", mTime))
	return
}
