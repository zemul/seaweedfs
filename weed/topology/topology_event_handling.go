package topology

import (
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"
	"math/rand"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

func (t *Topology) StartRefreshWritableVolumes(grpcDialOption grpc.DialOption, garbageThreshold float64, growThreshold float64, preallocate int64) {
	go func() {
		for {
			if t.IsLeader() {
				freshThreshHold := time.Now().Unix() - 3*t.pulse //3 times of sleep interval
				t.CollectDeadNodeAndFullVolumes(freshThreshHold, t.volumeSizeLimit, growThreshold)
			}
			time.Sleep(time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	go func(garbageThreshold float64) {
		c := time.Tick(15 * time.Minute)
		for _ = range c {
			if t.IsLeader() {
				t.Vacuum(grpcDialOption, garbageThreshold, 0, "", preallocate)
			}
		}
	}(garbageThreshold)
	go func() {
		for {
			select {
			case fv := <-t.chanFullVolumes:
				t.SetVolumeCapacityFull(fv)
			case cv := <-t.chanCrowdedVolumes:
				t.SetVolumeCrowded(cv)
			}
		}
	}()
}
func (t *Topology) SetVolumeCapacityFull(volumeInfo storage.VolumeInfo) bool {
	diskType := types.ToDiskType(volumeInfo.DiskType)
	vl := t.GetVolumeLayout(volumeInfo.Collection, volumeInfo.ReplicaPlacement, volumeInfo.Ttl, diskType)
	if !vl.SetVolumeCapacityFull(volumeInfo.Id) {
		return false
	}

	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	vidLocations, found := vl.vid2location[volumeInfo.Id]
	if !found {
		return false
	}

	for _, dn := range vidLocations.list {
		if !volumeInfo.ReadOnly {

			disk := dn.getOrCreateDisk(volumeInfo.DiskType)
			deltaDiskUsages := newDiskUsages()
			deltaDiskUsage := deltaDiskUsages.getOrCreateDisk(types.ToDiskType(volumeInfo.DiskType))
			deltaDiskUsage.activeVolumeCount = -1
			disk.UpAdjustDiskUsageDelta(deltaDiskUsages)

		}
	}
	return true
}

func (t *Topology) SetVolumeCrowded(volumeInfo storage.VolumeInfo) {
	diskType := types.ToDiskType(volumeInfo.DiskType)
	vl := t.GetVolumeLayout(volumeInfo.Collection, volumeInfo.ReplicaPlacement, volumeInfo.Ttl, diskType)
	vl.SetVolumeCrowded(volumeInfo.Id)
}

func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	for _, v := range dn.GetVolumes() {
		glog.V(0).Infoln("Removing Volume", v.Id, "from the dead volume server", dn.Id())
		diskType := types.ToDiskType(v.DiskType)
		vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
		vl.SetVolumeUnavailable(dn, v.Id)
	}

	negativeUsages := dn.GetDiskUsages().negative()
	dn.UpAdjustDiskUsageDelta(negativeUsages)
	dn.DeltaUpdateVolumes([]storage.VolumeInfo{}, dn.GetVolumes())
	dn.DeltaUpdateEcShards([]*erasure_coding.EcVolumeInfo{}, dn.GetEcShards())
	if dn.Parent() != nil {
		dn.Parent().UnlinkChildNode(dn.Id())
	}
}
