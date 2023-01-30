package mount

import (
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"os"
	"sync"
)

type FileHandleId uint64

var IsDebugFileReadWrite = false

type FileHandle struct {
	fh              FileHandleId
	counter         int64
	entry           *LockedEntry
	entryLock       sync.Mutex
	entryChunkGroup *filer.ChunkGroup
	inode           uint64
	wfs             *WFS

	// cache file has been written to
	dirtyMetadata bool
	dirtyPages    *PageWriter
	reader        *filer.ChunkReadAt
	contentType   string
	handle        uint64
	sync.Mutex

	isDeleted bool

	// for debugging
	mirrorFile *os.File
}

func newFileHandle(wfs *WFS, handleId FileHandleId, inode uint64, entry *filer_pb.Entry) *FileHandle {
	fh := &FileHandle{
		fh:      handleId,
		counter: 1,
		inode:   inode,
		wfs:     wfs,
	}
	// dirtyPages: newContinuousDirtyPages(file, writeOnly),
	fh.dirtyPages = newPageWriter(fh, wfs.option.ChunkSizeLimit)
	fh.entry = &LockedEntry{
		Entry: entry,
	}
	if entry != nil {
		fh.SetEntry(entry)
	}

	if IsDebugFileReadWrite {
		var err error
		fh.mirrorFile, err = os.OpenFile("/tmp/sw/"+entry.Name, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			println("failed to create mirror:", err.Error())
		}
	}

	return fh
}

func (fh *FileHandle) FullPath() util.FullPath {
	fp, _ := fh.wfs.inodeToPath.GetPath(fh.inode)
	return fp
}

func (fh *FileHandle) GetEntry() *filer_pb.Entry {
	return fh.entry.GetEntry()
}

func (fh *FileHandle) SetEntry(entry *filer_pb.Entry) {
	if entry != nil {
		fileSize := filer.FileSize(entry)
		entry.Attributes.FileSize = fileSize
		var resolveManifestErr error
		fh.entryChunkGroup, resolveManifestErr = filer.NewChunkGroup(fh.wfs.LookupFn(), fh.wfs.chunkCache, entry.Chunks)
		if resolveManifestErr != nil {
			glog.Warningf("failed to resolve manifest chunks in %+v", entry)
		}
	} else {
		glog.Fatalf("setting file handle entry to nil")
	}
	fh.entry.SetEntry(entry)
}

func (fh *FileHandle) UpdateEntry(fn func(entry *filer_pb.Entry)) *filer_pb.Entry {
	return fh.entry.UpdateEntry(fn)
}

func (fh *FileHandle) AddChunks(chunks []*filer_pb.FileChunk) {
	fh.entryLock.Lock()
	defer fh.entryLock.Unlock()

	if fh.entry == nil {
		return
	}

	fh.entry.AppendChunks(chunks)
}

func (fh *FileHandle) ReleaseHandle() {
	fh.entryLock.Lock()
	defer fh.entryLock.Unlock()

	fh.dirtyPages.Destroy()
	if IsDebugFileReadWrite {
		fh.mirrorFile.Close()
	}
}

func lessThan(a, b *filer_pb.FileChunk) bool {
	if a.ModifiedTsNs == b.ModifiedTsNs {
		return a.Fid.FileKey < b.Fid.FileKey
	}
	return a.ModifiedTsNs < b.ModifiedTsNs
}
