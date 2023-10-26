//go:build linux || darwin
// +build linux darwin

package command

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/denisbrodbeck/machineid"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/mount/unmount"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc/reflection"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

func runMount(cmd *Command, args []string) bool {

	if *mountOptions.pprof {
		go http.ListenAndServe(fmt.Sprintf(":%d", *mountOptions.debugPort), nil)
	}

	grace.SetupProfiling(*mountCpuProfile, *mountMemProfile)
	if *mountReadRetryTime < time.Second {
		*mountReadRetryTime = time.Second
	}
	util.RetryWaitTime = *mountReadRetryTime

	umask, umaskErr := strconv.ParseUint(*mountOptions.umaskString, 8, 64)
	if umaskErr != nil {
		fmt.Printf("can not parse umask %s", *mountOptions.umaskString)
		return false
	}

	if len(args) > 0 {
		return false
	}

	return RunMount(&mountOptions, os.FileMode(umask))
}

func RunMount(option *MountOptions, umask os.FileMode) bool {

	// basic checks
	chunkSizeLimitMB := *mountOptions.chunkSizeLimitMB
	if chunkSizeLimitMB <= 0 {
		fmt.Printf("Please specify a reasonable buffer size.")
		return false
	}

	*option.filerMountRootPath = filepath.Join(*option.filerMountRootPath)
	// galaxy
	var isSuper bool
	var ciphertext []byte
	var insName string
	userMountRootPath := strings.TrimPrefix(*option.filerMountRootPath, "/buckets")
	userMountRootPath = strings.TrimPrefix(userMountRootPath, "/")

	u, _ := user.Current()
	_, err := os.Stat(filepath.Join(u.HomeDir, ".galaxy", "csi"))
	if err == nil {
		// admin user
		isSuper = true
		ciphertext, _ = os.ReadFile(filepath.Join(u.HomeDir, ".galaxy", "csi"))
	}

	if !isSuper {
		// normal user
		if userMountRootPath == "" {
			fmt.Printf("Please check that the mount directory is correct")
			return false
		}
		idx := strings.Index(userMountRootPath, "/")
		if idx == -1 {
			insName = userMountRootPath
		} else {
			insName = userMountRootPath[:idx]
		}
		ciphertext, err = os.ReadFile(filepath.Join(u.HomeDir, ".galaxy", insName))
		if err != nil {
			fmt.Printf("fielad read  signature file %v", err)
			return true
		}
	}

	data, err := util.Decrypt(ciphertext, cipherKey)
	if err != nil {
		fmt.Printf("Signature verification failure")
		return true
	}
	msg := strings.Split(string(data), ",") // secretKey, sourceMid, addr
	secret, sourceMid, addr := msg[0], msg[1], msg[2]
	mid, _ := machineid.ID()
	if mid != sourceMid {
		fmt.Printf("Machine without authorization")
		return true
	}

	if isSuper {
		if secret != superSecret {
			fmt.Println("super token unauthorized")
			return true
		}
	} else {
		resp, e := http.DefaultClient.Get(addr + fmt.Sprintf("/o/mountCheck?ins_name=%s&secret=%s", insName, secret))
		if e != nil {
			glog.V(3).Infof("mountCheck err: %v", err)
			fmt.Printf("%s: token check timeout\n", insName)
			return true
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			fmt.Println(string(body))
			return true
		}
		body := make(map[string]string)
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			fmt.Println(err)
			return true
		}
		if _, has := body["rootPath"]; !has {
			fmt.Println("get remote config error")
			return true
		}

		*option.filerMountRootPath = filepath.Join(body["rootPath"], userMountRootPath)

		// get remote addr
		if *option.filer == "localhost:8888" {
			*option.filer = body["filers"]
		}
	}

	// try to connect to filer
	filerAddresses := pb.ServerAddresses(*option.filer).ToAddresses()
	util.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	var cipher bool
	//var err error
	for i := 0; i < 10; i++ {
		err = pb.WithOneOfGrpcFilerClients(false, filerAddresses, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer grpc address %v configuration: %v", filerAddresses, err)
			}
			cipher = resp.Cipher
			return nil
		})
		if err != nil {
			glog.V(0).Infof("failed to talk to filer %v: %v", filerAddresses, err)
			glog.V(0).Infof("wait for %d seconds ...", i+1)
			time.Sleep(time.Duration(i+1) * time.Second)
		}
	}
	if err != nil {
		glog.Errorf("failed to talk to filer %v: %v", filerAddresses, err)
		return true
	}

	// clean up mount point
	dir := util.ResolvePath(*option.dir)
	if dir == "" {
		fmt.Printf("Please specify the mount directory via \"-dir\"")
		return false
	}

	unmount.Unmount(dir)

	// start on local unix socket
	if *option.localSocket == "" {
		mountDirHash := util.HashToInt32([]byte(dir))
		if mountDirHash < 0 {
			mountDirHash = -mountDirHash
		}
		*option.localSocket = fmt.Sprintf("/tmp/seaweedfs-mount-%d.sock", mountDirHash)
	}
	if err := os.Remove(*option.localSocket); err != nil && !os.IsNotExist(err) {
		glog.Fatalf("Failed to remove %s, error: %s", *option.localSocket, err.Error())
	}
	montSocketListener, err := net.Listen("unix", *option.localSocket)
	if err != nil {
		glog.Fatalf("Failed to listen on %s: %v", *option.localSocket, err)
	}

	// detect mount folder mode
	if *option.dirAutoCreate {
		os.MkdirAll(dir, os.FileMode(0777)&^umask)
	}
	fileInfo, err := os.Stat(dir)

	// collect uid, gid
	uid, gid := uint32(0), uint32(0)
	mountMode := os.ModeDir | 0777
	if err == nil {
		mountMode = os.ModeDir | os.FileMode(0777)&^umask
		uid, gid = util.GetFileUidGid(fileInfo)
		fmt.Printf("mount point owner uid=%d gid=%d mode=%s\n", uid, gid, mountMode)
	} else {
		fmt.Printf("can not stat %s\n", dir)
		return false
	}

	// detect uid, gid
	if uid == 0 {
		if u, err := user.Current(); err == nil {
			if parsedId, pe := strconv.ParseUint(u.Uid, 10, 32); pe == nil {
				uid = uint32(parsedId)
			}
			if parsedId, pe := strconv.ParseUint(u.Gid, 10, 32); pe == nil {
				gid = uint32(parsedId)
			}
			fmt.Printf("current uid=%d gid=%d\n", uid, gid)
		}
	}

	// mapping uid, gid
	uidGidMapper, err := meta_cache.NewUidGidMapper(*option.uidMap, *option.gidMap)
	if err != nil {
		fmt.Printf("failed to parse %s %s: %v\n", *option.uidMap, *option.gidMap, err)
		return false
	}

	// Ensure target mount point availability
	if isValid := checkMountPointAvailable(dir); !isValid {
		glog.Fatalf("Target mount point is not available: %s, please check!", dir)
		return true
	}

	serverFriendlyName := strings.ReplaceAll(*option.filer, ",", "+")

	// mount fuse
	fuseMountOptions := &fuse.MountOptions{
		AllowOther:               *option.allowOthers,
		Options:                  option.extraOptions,
		MaxBackground:            128,
		MaxWrite:                 1024 * 1024 * 2,
		MaxReadAhead:             1024 * 1024 * 2,
		IgnoreSecurityLabels:     false,
		RememberInodes:           false,
		FsName:                   serverFriendlyName + ":" + *option.filerMountRootPath,
		Name:                     "seaweedfs",
		SingleThreaded:           false,
		DisableXAttrs:            *option.disableXAttr,
		Debug:                    *option.debug,
		EnableLocks:              false,
		ExplicitDataCacheControl: false,
		DirectMount:              true,
		DirectMountFlags:         0,
		//SyncRead:                 false, // set to false to enable the FUSE_CAP_ASYNC_READ capability
		EnableAcl: true,
	}
	if *option.nonempty {
		fuseMountOptions.Options = append(fuseMountOptions.Options, "nonempty")
	}
	if *option.readOnly {
		if runtime.GOOS == "darwin" {
			fuseMountOptions.Options = append(fuseMountOptions.Options, "rdonly")
		} else {
			fuseMountOptions.Options = append(fuseMountOptions.Options, "ro")
		}
	}
	if runtime.GOOS == "darwin" {
		// https://github-wiki-see.page/m/macfuse/macfuse/wiki/Mount-Options
		ioSizeMB := 1
		for ioSizeMB*2 <= *option.chunkSizeLimitMB && ioSizeMB*2 <= 32 {
			ioSizeMB *= 2
		}
		fuseMountOptions.Options = append(fuseMountOptions.Options, "daemon_timeout=600")
		fuseMountOptions.Options = append(fuseMountOptions.Options, "noapplexattr")
		// fuseMountOptions.Options = append(fuseMountOptions.Options, "novncache") // need to test effectiveness
		fuseMountOptions.Options = append(fuseMountOptions.Options, "slow_statfs")
		fuseMountOptions.Options = append(fuseMountOptions.Options, "volname="+serverFriendlyName)
		fuseMountOptions.Options = append(fuseMountOptions.Options, fmt.Sprintf("iosize=%d", ioSizeMB*1024*1024))
	}

	// find mount point
	mountRoot := *option.filerMountRootPath
	if mountRoot != "/" && strings.HasSuffix(mountRoot, "/") {
		mountRoot = mountRoot[0 : len(mountRoot)-1]
	}

	seaweedFileSystem := mount.NewSeaweedFileSystem(&mount.Option{
		MountDirectory:     dir,
		FilerAddresses:     filerAddresses,
		GrpcDialOption:     grpcDialOption,
		FilerMountRootPath: mountRoot,
		Collection:         *option.collection,
		Replication:        *option.replication,
		TtlSec:             int32(*option.ttlSec),
		DiskType:           types.ToDiskType(*option.diskType),
		ChunkSizeLimit:     int64(chunkSizeLimitMB) * 1024 * 1024,
		ConcurrentWriters:  *option.concurrentWriters,
		CacheDirForRead:    *option.cacheDirForRead,
		CacheSizeMBForRead: *option.cacheSizeMBForRead,
		CacheDirForWrite:   *option.cacheDirForWrite,
		DataCenter:         *option.dataCenter,
		Quota:              int64(*option.collectionQuota) * 1024 * 1024,
		MountUid:           uid,
		MountGid:           gid,
		MountMode:          mountMode,
		MountCtime:         fileInfo.ModTime(),
		MountMtime:         time.Now(),
		Umask:              umask,
		VolumeServerAccess: *mountOptions.volumeServerAccess,
		Cipher:             cipher,
		UidGidMapper:       uidGidMapper,
		DisableXAttr:       *option.disableXAttr,
	})

	// create mount root
	mountRootPath := util.FullPath(mountRoot)
	mountRootParent, mountDir := mountRootPath.DirAndName()
	if err = filer_pb.Mkdir(seaweedFileSystem, mountRootParent, mountDir, nil); err != nil {
		fmt.Printf("failed to create dir %s on filer %s: %v\n", mountRoot, filerAddresses, err)
		return false
	}

	server, err := fuse.NewServer(seaweedFileSystem, dir, fuseMountOptions)
	if err != nil {
		glog.Fatalf("Mount fail: %v", err)
	}
	grace.OnInterrupt(func() {
		unmount.Unmount(dir)
	})

	grpcS := pb.NewGrpcServer()
	mount_pb.RegisterSeaweedMountServer(grpcS, seaweedFileSystem)
	reflection.Register(grpcS)
	go grpcS.Serve(montSocketListener)

	seaweedFileSystem.StartBackgroundTasks()

	glog.V(0).Infof("mounted %s %s to %v", *option.filer, mountRoot, dir)
	glog.V(0).Infof("This is SeaweedFS version %s %s %s", util.Version(), runtime.GOOS, runtime.GOARCH)

	server.Serve()

	return true
}
