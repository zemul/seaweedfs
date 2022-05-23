package page_writer

import (
	"io"
)

type SaveToStorageFunc func(reader io.Reader, offset int64, size int64, cleanupFn func())

type PageChunk interface {
	FreeResource()
	WriteDataAt(src []byte, offset int64) (n int)
	ReadDataAt(p []byte, off int64) (maxStop int64)
	IsComplete() bool
	WrittenSize() int64
	SaveContent(saveFn SaveToStorageFunc)
}
