package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {

	glog.V(4).Infof("LookupDirectoryEntry %s", filepath.Join(req.Directory, req.Name))

	entry, err := fs.filer.FindEntry(ctx, util.JoinPath(req.Directory, req.Name))
	if err == filer_pb.ErrNotFound {
		return &filer_pb.LookupDirectoryEntryResponse{}, err
	}
	if err != nil {
		glog.V(3).Infof("LookupDirectoryEntry %s: %+v, ", filepath.Join(req.Directory, req.Name), err)
		return nil, err
	}

	return &filer_pb.LookupDirectoryEntryResponse{
		Entry: entry.ToProtoEntry(),
	}, nil
}

func (fs *FilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) (err error) {

	glog.V(4).Infof("ListEntries %v", req)

	limit := int(req.Limit)
	if limit == 0 {
		limit = fs.option.DirListingLimit
	}

	paginationLimit := filer.PaginationSize
	if limit < paginationLimit {
		paginationLimit = limit
	}

	lastFileName := req.StartFromFileName
	includeLastFile := req.InclusiveStartFrom
	var listErr error
	for limit > 0 {
		var hasEntries bool
		lastFileName, listErr = fs.filer.StreamListDirectoryEntries(stream.Context(), util.FullPath(req.Directory), lastFileName, includeLastFile, int64(paginationLimit), req.Prefix, "", "", func(entry *filer.Entry) bool {
			hasEntries = true
			if err = stream.Send(&filer_pb.ListEntriesResponse{
				Entry: entry.ToProtoEntry(),
			}); err != nil {
				return false
			}

			limit--
			if limit == 0 {
				return false
			}
			return true
		})

		if listErr != nil {
			return listErr
		}
		if err != nil {
			return err
		}
		if !hasEntries {
			return nil
		}

		includeLastFile = false

	}

	return nil
}

func (fs *FilerServer) LookupVolume(ctx context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {

	resp := &filer_pb.LookupVolumeResponse{
		LocationsMap: make(map[string]*filer_pb.Locations),
	}

	for _, vidString := range req.VolumeIds {
		vid, err := strconv.Atoi(vidString)
		if err != nil {
			glog.V(1).Infof("Unknown volume id %d", vid)
			return nil, err
		}
		var locs []*filer_pb.Location
		locations, found := fs.filer.MasterClient.GetLocations(uint32(vid))
		if !found {
			continue
		}
		for _, loc := range locations {
			locs = append(locs, &filer_pb.Location{
				Url:       loc.Url,
				PublicUrl: loc.PublicUrl,
				GrpcPort:  uint32(loc.GrpcPort),
			})
		}
		resp.LocationsMap[vidString] = &filer_pb.Locations{
			Locations: locs,
		}
	}

	return resp, nil
}

func (fs *FilerServer) lookupFileId(fileId string) (targetUrls []string, err error) {
	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		return nil, err
	}
	locations, found := fs.filer.MasterClient.GetLocations(uint32(fid.VolumeId))
	if !found || len(locations) == 0 {
		return nil, fmt.Errorf("not found volume %d in %s", fid.VolumeId, fileId)
	}
	for _, loc := range locations {
		targetUrls = append(targetUrls, fmt.Sprintf("http://%s/%s", loc.Url, fileId))
	}
	return
}

func (fs *FilerServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (resp *filer_pb.CreateEntryResponse, err error) {

	glog.V(4).Infof("CreateEntry %v/%v", req.Directory, req.Entry.Name)

	resp = &filer_pb.CreateEntryResponse{}

	chunks, garbage, err2 := fs.cleanupChunks(util.Join(req.Directory, req.Entry.Name), nil, req.Entry)
	if err2 != nil {
		return &filer_pb.CreateEntryResponse{}, fmt.Errorf("CreateEntry cleanupChunks %s %s: %v", req.Directory, req.Entry.Name, err2)
	}

	newEntry := filer.FromPbEntry(req.Directory, req.Entry)
	newEntry.Chunks = chunks

	createErr := fs.filer.CreateEntry(ctx, newEntry, req.OExcl, req.IsFromOtherCluster, req.Signatures)

	if createErr == nil {
		fs.filer.DeleteChunks(garbage)
	} else {
		glog.V(3).Infof("CreateEntry %s: %v", filepath.Join(req.Directory, req.Entry.Name), createErr)
		resp.Error = createErr.Error()
	}

	return
}

func (fs *FilerServer) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {

	glog.V(4).Infof("UpdateEntry %v", req)

	fullpath := util.Join(req.Directory, req.Entry.Name)
	entry, err := fs.filer.FindEntry(ctx, util.FullPath(fullpath))
	if err != nil {
		return &filer_pb.UpdateEntryResponse{}, fmt.Errorf("not found %s: %v", fullpath, err)
	}

	chunks, garbage, err2 := fs.cleanupChunks(fullpath, entry, req.Entry)
	if err2 != nil {
		return &filer_pb.UpdateEntryResponse{}, fmt.Errorf("UpdateEntry cleanupChunks %s: %v", fullpath, err2)
	}

	newEntry := filer.FromPbEntry(req.Directory, req.Entry)
	newEntry.Chunks = chunks

	if filer.EqualEntry(entry, newEntry) {
		return &filer_pb.UpdateEntryResponse{}, err
	}

	if err = fs.filer.UpdateEntry(ctx, entry, newEntry); err == nil {
		fs.filer.DeleteChunks(garbage)

		fs.filer.NotifyUpdateEvent(ctx, entry, newEntry, true, req.IsFromOtherCluster, req.Signatures)

	} else {
		glog.V(3).Infof("UpdateEntry %s: %v", filepath.Join(req.Directory, req.Entry.Name), err)
	}

	return &filer_pb.UpdateEntryResponse{}, err
}

func (fs *FilerServer) cleanupChunks(fullpath string, existingEntry *filer.Entry, newEntry *filer_pb.Entry) (chunks, garbage []*filer_pb.FileChunk, err error) {

	// remove old chunks if not included in the new ones
	if existingEntry != nil {
		garbage, err = filer.MinusChunks(fs.lookupFileId, existingEntry.Chunks, newEntry.Chunks)
		if err != nil {
			return newEntry.Chunks, nil, fmt.Errorf("MinusChunks: %v", err)
		}
	}

	// files with manifest chunks are usually large and append only, skip calculating covered chunks
	manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(newEntry.Chunks)

	chunks, coveredChunks := filer.CompactFileChunks(fs.lookupFileId, nonManifestChunks)
	garbage = append(garbage, coveredChunks...)

	if newEntry.Attributes != nil {
		so, _ := fs.detectStorageOption(fullpath,
			newEntry.Attributes.Collection,
			newEntry.Attributes.Replication,
			newEntry.Attributes.TtlSec,
			newEntry.Attributes.DiskType,
			"",
			"",
			"",
		) // ignore readonly error for capacity needed to manifestize
		chunks, err = filer.MaybeManifestize(fs.saveAsChunk(so), chunks)
		if err != nil {
			// not good, but should be ok
			glog.V(0).Infof("MaybeManifestize: %v", err)
		}
	}

	chunks = append(chunks, manifestChunks...)

	return
}

func (fs *FilerServer) AppendToEntry(ctx context.Context, req *filer_pb.AppendToEntryRequest) (*filer_pb.AppendToEntryResponse, error) {

	glog.V(4).Infof("AppendToEntry %v", req)

	fullpath := util.NewFullPath(req.Directory, req.EntryName)
	var offset int64 = 0
	entry, err := fs.filer.FindEntry(ctx, fullpath)
	if err == filer_pb.ErrNotFound {
		entry = &filer.Entry{
			FullPath: fullpath,
			Attr: filer.Attr{
				Crtime: time.Now(),
				Mtime:  time.Now(),
				Mode:   os.FileMode(0644),
				Uid:    OS_UID,
				Gid:    OS_GID,
			},
		}
	} else {
		offset = int64(filer.TotalSize(entry.Chunks))
	}

	for _, chunk := range req.Chunks {
		chunk.Offset = offset
		offset += int64(chunk.Size)
	}

	entry.Chunks = append(entry.Chunks, req.Chunks...)
	so, err := fs.detectStorageOption(string(fullpath), entry.Collection, entry.Replication, entry.TtlSec, entry.DiskType, "", "", "")
	if err != nil {
		glog.Warningf("detectStorageOption: %v", err)
		return &filer_pb.AppendToEntryResponse{}, err
	}
	entry.Chunks, err = filer.MaybeManifestize(fs.saveAsChunk(so), entry.Chunks)
	if err != nil {
		// not good, but should be ok
		glog.V(0).Infof("MaybeManifestize: %v", err)
	}

	err = fs.filer.CreateEntry(context.Background(), entry, false, false, nil)

	return &filer_pb.AppendToEntryResponse{}, err
}

func (fs *FilerServer) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (resp *filer_pb.DeleteEntryResponse, err error) {

	glog.V(4).Infof("DeleteEntry %v", req)

	err = fs.filer.DeleteEntryMetaAndData(ctx, util.JoinPath(req.Directory, req.Name), req.IsRecursive, req.IgnoreRecursiveError, req.IsDeleteData, req.IsFromOtherCluster, req.Signatures)
	resp = &filer_pb.DeleteEntryResponse{}
	if err != nil && err != filer_pb.ErrNotFound {
		resp.Error = err.Error()
	}
	return resp, nil
}

func (fs *FilerServer) AssignVolume(ctx context.Context, req *filer_pb.AssignVolumeRequest) (resp *filer_pb.AssignVolumeResponse, err error) {

	so, err := fs.detectStorageOption(req.Path, req.Collection, req.Replication, req.TtlSec, req.DiskType, req.DataCenter, req.Rack, req.DataNode)
	if err != nil {
		glog.V(3).Infof("AssignVolume: %v", err)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume: %v", err)}, nil
	}

	assignRequest, altRequest := so.ToAssignRequests(int(req.Count))

	assignResult, err := operation.Assign(fs.filer.GetMaster, fs.grpcDialOption, assignRequest, altRequest)
	if err != nil {
		glog.V(3).Infof("AssignVolume: %v", err)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume: %v", err)}, nil
	}
	if assignResult.Error != "" {
		glog.V(3).Infof("AssignVolume error: %v", assignResult.Error)
		return &filer_pb.AssignVolumeResponse{Error: fmt.Sprintf("assign volume result: %v", assignResult.Error)}, nil
	}

	return &filer_pb.AssignVolumeResponse{
		FileId: assignResult.Fid,
		Count:  int32(assignResult.Count),
		Location: &filer_pb.Location{
			Url:       assignResult.Url,
			PublicUrl: assignResult.PublicUrl,
			GrpcPort:  uint32(assignResult.GrpcPort),
		},
		Auth:        string(assignResult.Auth),
		Collection:  so.Collection,
		Replication: so.Replication,
	}, nil
}

func (fs *FilerServer) CollectionList(ctx context.Context, req *filer_pb.CollectionListRequest) (resp *filer_pb.CollectionListResponse, err error) {

	glog.V(4).Infof("CollectionList %v", req)
	resp = &filer_pb.CollectionListResponse{}

	err = fs.filer.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		masterResp, err := client.CollectionList(context.Background(), &master_pb.CollectionListRequest{
			IncludeNormalVolumes: req.IncludeNormalVolumes,
			IncludeEcVolumes:     req.IncludeEcVolumes,
		})
		if err != nil {
			return err
		}
		for _, c := range masterResp.Collections {
			resp.Collections = append(resp.Collections, &filer_pb.Collection{Name: c.Name})
		}
		return nil
	})

	return
}

func (fs *FilerServer) DeleteCollection(ctx context.Context, req *filer_pb.DeleteCollectionRequest) (resp *filer_pb.DeleteCollectionResponse, err error) {

	glog.V(4).Infof("DeleteCollection %v", req)

	err = fs.filer.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err := client.CollectionDelete(context.Background(), &master_pb.CollectionDeleteRequest{
			Name: req.GetCollection(),
		})
		return err
	})

	return &filer_pb.DeleteCollectionResponse{}, err
}

func (fs *FilerServer) Statistics(ctx context.Context, req *filer_pb.StatisticsRequest) (resp *filer_pb.StatisticsResponse, err error) {

	var output *master_pb.StatisticsResponse

	err = fs.filer.MasterClient.WithClient(false, func(masterClient master_pb.SeaweedClient) error {
		grpcResponse, grpcErr := masterClient.Statistics(context.Background(), &master_pb.StatisticsRequest{
			Replication: req.Replication,
			Collection:  req.Collection,
			Ttl:         req.Ttl,
			DiskType:    req.DiskType,
		})
		if grpcErr != nil {
			return grpcErr
		}

		output = grpcResponse
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &filer_pb.StatisticsResponse{
		TotalSize: output.TotalSize,
		UsedSize:  output.UsedSize,
		FileCount: output.FileCount,
	}, nil
}

func (fs *FilerServer) GetFilerConfiguration(ctx context.Context, req *filer_pb.GetFilerConfigurationRequest) (resp *filer_pb.GetFilerConfigurationResponse, err error) {

	clusterId, _ := fs.filer.Store.KvGet(context.Background(), []byte("clusterId"))

	t := &filer_pb.GetFilerConfigurationResponse{
		Masters:            pb.ToAddressStrings(fs.option.Masters),
		Collection:         fs.option.Collection,
		Replication:        fs.option.DefaultReplication,
		MaxMb:              uint32(fs.option.MaxMB),
		DirBuckets:         fs.filer.DirBucketsPath,
		Cipher:             fs.filer.Cipher,
		Signature:          fs.filer.Signature,
		MetricsAddress:     fs.metricsAddress,
		MetricsIntervalSec: int32(fs.metricsIntervalSec),
		Version:            util.Version(),
		ClusterId:          string(clusterId),
	}

	glog.V(4).Infof("GetFilerConfiguration: %v", t)

	return t, nil
}

func (fs *FilerServer) KeepConnected(stream filer_pb.SeaweedFiler_KeepConnectedServer) error {

	req, err := stream.Recv()
	if err != nil {
		return err
	}

	clientName := util.JoinHostPort(req.Name, int(req.GrpcPort))
	m := make(map[string]bool)
	for _, tp := range req.Resources {
		m[tp] = true
	}
	fs.brokersLock.Lock()
	fs.brokers[clientName] = m
	glog.V(0).Infof("+ broker %v", clientName)
	fs.brokersLock.Unlock()

	defer func() {
		fs.brokersLock.Lock()
		delete(fs.brokers, clientName)
		glog.V(0).Infof("- broker %v: %v", clientName, err)
		fs.brokersLock.Unlock()
	}()

	for {
		if err := stream.Send(&filer_pb.KeepConnectedResponse{}); err != nil {
			glog.V(0).Infof("send broker %v: %+v", clientName, err)
			return err
		}
		// println("replied")

		if _, err := stream.Recv(); err != nil {
			glog.V(0).Infof("recv broker %v: %v", clientName, err)
			return err
		}
		// println("received")
	}

}

func (fs *FilerServer) LocateBroker(ctx context.Context, req *filer_pb.LocateBrokerRequest) (resp *filer_pb.LocateBrokerResponse, err error) {

	resp = &filer_pb.LocateBrokerResponse{}

	fs.brokersLock.Lock()
	defer fs.brokersLock.Unlock()

	var localBrokers []*filer_pb.LocateBrokerResponse_Resource

	for b, m := range fs.brokers {
		if _, found := m[req.Resource]; found {
			resp.Found = true
			resp.Resources = []*filer_pb.LocateBrokerResponse_Resource{
				{
					GrpcAddresses: b,
					ResourceCount: int32(len(m)),
				},
			}
			return
		}
		localBrokers = append(localBrokers, &filer_pb.LocateBrokerResponse_Resource{
			GrpcAddresses: b,
			ResourceCount: int32(len(m)),
		})
	}

	resp.Resources = localBrokers

	return resp, nil

}
