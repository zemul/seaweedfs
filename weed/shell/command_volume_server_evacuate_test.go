package shell

import (
	"os"
	"testing"
)

func TestVolumeServerEvacuate(t *testing.T) {
	c := commandVolumeServerEvacuate{}
	c.topologyInfo = parseOutput(topoData)

	volumeServer := "192.168.1.4:8080"
	if err := c.evacuateNormalVolumes(nil, volumeServer, true, false, os.Stdout); err != nil {
		t.Errorf("evacuate: %v", err)
	}

}
