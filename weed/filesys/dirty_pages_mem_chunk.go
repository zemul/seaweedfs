package filesys

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filesys/page_writer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"io"
	"sync"
	"time"
)

type MemoryChunkPages struct {
	fh             *FileHandle
	writeWaitGroup sync.WaitGroup
	chunkAddLock   sync.Mutex
	lastErr        error
	collection     string
	replication    string
	uploadPipeline *page_writer.UploadPipeline
	hasWrites      bool
}

var (
	_ = page_writer.DirtyPages(&MemoryChunkPages{})
)

func newMemoryChunkPages(fh *FileHandle, chunkSize int64) *MemoryChunkPages {

	dirtyPages := &MemoryChunkPages{
		fh: fh,
	}

	dirtyPages.uploadPipeline = page_writer.NewUploadPipeline(fh.f.fullpath(),
		fh.f.wfs.concurrentWriters, chunkSize, dirtyPages.saveChunkedFileIntevalToStorage)

	return dirtyPages
}

func (pages *MemoryChunkPages) AddPage(offset int64, data []byte) {
	pages.hasWrites = true

	glog.V(4).Infof("%v memory AddPage [%d, %d)", pages.fh.f.fullpath(), offset, offset+int64(len(data)))
	pages.uploadPipeline.SaveDataAt(data, offset)

	return
}

func (pages *MemoryChunkPages) FlushData() error {
	if !pages.hasWrites {
		return nil
	}
	pages.uploadPipeline.FlushAll()
	if pages.lastErr != nil {
		return fmt.Errorf("flush data: %v", pages.lastErr)
	}
	return nil
}

func (pages *MemoryChunkPages) ReadDirtyDataAt(data []byte, startOffset int64) (maxStop int64) {
	if !pages.hasWrites {
		return
	}
	return pages.uploadPipeline.MaybeReadDataAt(data, startOffset)
}

func (pages *MemoryChunkPages) GetStorageOptions() (collection, replication string) {
	return pages.collection, pages.replication
}

func (pages *MemoryChunkPages) saveChunkedFileIntevalToStorage(reader io.Reader, offset int64, size int64, cleanupFn func()) {

	mtime := time.Now().UnixNano()
	defer cleanupFn()

	chunk, collection, replication, err := pages.fh.f.wfs.saveDataAsChunk(pages.fh.f.fullpath())(reader, pages.fh.f.Name, offset)
	if err != nil {
		glog.V(0).Infof("%s saveToStorage [%d,%d): %v", pages.fh.f.fullpath(), offset, offset+size, err)
		pages.lastErr = err
		return
	}
	chunk.Mtime = mtime
	pages.collection, pages.replication = collection, replication
	pages.chunkAddLock.Lock()
	pages.fh.f.addChunks([]*filer_pb.FileChunk{chunk})
	pages.fh.entryViewCache = nil
	glog.V(3).Infof("%s saveToStorage %s [%d,%d)", pages.fh.f.fullpath(), chunk.FileId, offset, offset+size)
	pages.chunkAddLock.Unlock()

}

func (pages MemoryChunkPages) Destroy() {
	pages.uploadPipeline.Shutdown()
}

func (pages *MemoryChunkPages) LockForRead(startOffset, stopOffset int64) {
	pages.uploadPipeline.LockForRead(startOffset, stopOffset)
}
func (pages *MemoryChunkPages) UnlockForRead(startOffset, stopOffset int64) {
	pages.uploadPipeline.UnlockForRead(startOffset, stopOffset)
}
