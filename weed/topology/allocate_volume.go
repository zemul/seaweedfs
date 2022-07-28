package topology

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *DataNode, grpcDialOption grpc.DialOption, vid needle.VolumeId, option *VolumeGrowOption) error {

	return operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		_, allocateErr := client.AllocateVolume(context.Background(), &volume_server_pb.AllocateVolumeRequest{
			VolumeId:           uint32(vid),
			Collection:         option.Collection,
			Replication:        option.ReplicaPlacement.String(),
			Ttl:                option.Ttl.String(),
			Preallocate:        option.Preallocate,
			MemoryMapMaxSizeMb: option.MemoryMapMaxSizeMb,
			DiskType:           string(option.DiskType),
		})
		return allocateErr
	})

}
