package filer

import (
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

func LookupByMasterClientFn(masterClient *wdclient.MasterClient) func(vids []string) (map[string]*operation.LookupResult, error) {
	return func(vids []string) (map[string]*operation.LookupResult, error) {
		m := make(map[string]*operation.LookupResult)
		for _, vid := range vids {
			locs, _ := masterClient.GetVidLocations(vid)
			var locations []operation.Location
			for _, loc := range locs {
				locations = append(locations, operation.Location{
					Url:       loc.Url,
					PublicUrl: loc.PublicUrl,
				})
			}
			m[vid] = &operation.LookupResult{
				VolumeOrFileId: vid,
				Locations:      locations,
			}
		}
		return m, nil
	}
}

func (f *Filer) loopProcessingDeletion() {

	lookupFunc := LookupByMasterClientFn(f.MasterClient)

	DeletionBatchSize := 100000 // roughly 20 bytes cost per file id.

	var deletionCount int
	for {
		deletionCount = 0
		f.fileIdDeletionQueue.Consume(func(fileIds []string) {
			for len(fileIds) > 0 {
				var toDeleteFileIds []string
				if len(fileIds) > DeletionBatchSize {
					toDeleteFileIds = fileIds[:DeletionBatchSize]
					fileIds = fileIds[DeletionBatchSize:]
				} else {
					toDeleteFileIds = fileIds
					fileIds = fileIds[:0]
				}
				deletionCount = len(toDeleteFileIds)
				_, err := operation.DeleteFilesWithLookupVolumeId(f.GrpcDialOption, toDeleteFileIds, lookupFunc)
				if err != nil {
					if !strings.Contains(err.Error(), storage.ErrorDeleted.Error()) {
						glog.V(0).Infof("deleting fileIds len=%d error: %v", deletionCount, err)
					}
				} else {
					glog.V(2).Infof("deleting fileIds %+v", toDeleteFileIds)
				}
			}
		})

		if deletionCount == 0 {
			time.Sleep(1123 * time.Millisecond)
		}
	}
}

func (f *Filer) doDeleteFileIds(fileIds []string) {

	lookupFunc := LookupByMasterClientFn(f.MasterClient)
	DeletionBatchSize := 100000 // roughly 20 bytes cost per file id.

	for len(fileIds) > 0 {
		var toDeleteFileIds []string
		if len(fileIds) > DeletionBatchSize {
			toDeleteFileIds = fileIds[:DeletionBatchSize]
			fileIds = fileIds[DeletionBatchSize:]
		} else {
			toDeleteFileIds = fileIds
			fileIds = fileIds[:0]
		}
		deletionCount := len(toDeleteFileIds)
		_, err := operation.DeleteFilesWithLookupVolumeId(f.GrpcDialOption, toDeleteFileIds, lookupFunc)
		if err != nil {
			if !strings.Contains(err.Error(), storage.ErrorDeleted.Error()) {
				glog.V(0).Infof("deleting fileIds len=%d error: %v", deletionCount, err)
			}
		}
	}
}

func (f *Filer) DirectDeleteChunks(chunks []*filer_pb.FileChunk) {
	var fileIdsToDelete []string
	for _, chunk := range chunks {
		if !chunk.IsChunkManifest {
			fileIdsToDelete = append(fileIdsToDelete, chunk.GetFileIdString())
			continue
		}
		dataChunks, manifestResolveErr := ResolveOneChunkManifest(f.MasterClient.LookupFileId, chunk)
		if manifestResolveErr != nil {
			glog.V(0).Infof("failed to resolve manifest %s: %v", chunk.FileId, manifestResolveErr)
		}
		for _, dChunk := range dataChunks {
			fileIdsToDelete = append(fileIdsToDelete, dChunk.GetFileIdString())
		}
		fileIdsToDelete = append(fileIdsToDelete, chunk.GetFileIdString())
	}

	f.doDeleteFileIds(fileIdsToDelete)
}

func (f *Filer) DeleteChunks(chunks []*filer_pb.FileChunk) {
	for _, chunk := range chunks {
		if !chunk.IsChunkManifest {
			f.fileIdDeletionQueue.EnQueue(chunk.GetFileIdString())
			continue
		}
		dataChunks, manifestResolveErr := ResolveOneChunkManifest(f.MasterClient.LookupFileId, chunk)
		if manifestResolveErr != nil {
			glog.V(0).Infof("failed to resolve manifest %s: %v", chunk.FileId, manifestResolveErr)
		}
		for _, dChunk := range dataChunks {
			f.fileIdDeletionQueue.EnQueue(dChunk.GetFileIdString())
		}
		f.fileIdDeletionQueue.EnQueue(chunk.GetFileIdString())
	}
}

func (f *Filer) DeleteChunksNotRecursive(chunks []*filer_pb.FileChunk) {
	for _, chunk := range chunks {
		f.fileIdDeletionQueue.EnQueue(chunk.GetFileIdString())
	}
}

func (f *Filer) deleteChunksIfNotNew(oldEntry, newEntry *Entry) {
	var oldChunks, newChunks []*filer_pb.FileChunk
	if oldEntry != nil {
		oldChunks = oldEntry.GetChunks()
	}
	if newEntry != nil {
		newChunks = newEntry.GetChunks()
	}

	toDelete, err := MinusChunks(f.MasterClient.GetLookupFileIdFunction(), oldChunks, newChunks)
	if err != nil {
		glog.Errorf("Failed to resolve old entry chunks when delete old entry chunks. new: %s, old: %s",
			newChunks, oldChunks)
		return
	}
	f.DeleteChunksNotRecursive(toDelete)
}
