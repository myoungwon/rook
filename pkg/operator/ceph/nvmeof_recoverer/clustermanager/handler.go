package clustermanager

import (
	"strconv"

	"github.com/coreos/pkg/capnslog"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
)

var logger = capnslog.NewPackageLogger("github.com/rook/rook", "cluster-manager")

// ConnectionInfo contains the address and port of a fabric device
type ConnectionInfo struct {
	Address string
	Port    string
}

type ClusterManager struct {
	HostExists      map[string]bool
	AttachableHosts []string
	NqnEndpointMap  map[string]ConnectionInfo
}

func New() *ClusterManager {
	return &ClusterManager{
		HostExists:      make(map[string]bool),
		AttachableHosts: []string{},
		NqnEndpointMap:  make(map[string]ConnectionInfo),
	}
}

func (cm *ClusterManager) AddAttachbleHost(hostname string) error {
	if !cm.HostExists[hostname] {
		cm.AttachableHosts = append(cm.AttachableHosts, hostname)
		cm.HostExists[hostname] = true
	}

	return nil
}

func (cm *ClusterManager) UpdateDeviceEndpointeMap(nvmeofstorage *cephv1.NvmeOfStorage) {
	for _, device := range nvmeofstorage.Spec.Devices {
		cm.NqnEndpointMap[device.SubNQN] = ConnectionInfo{
			Address: nvmeofstorage.Spec.IP,
			Port:    strconv.Itoa(device.Port),
		}
	}
}

func (cm *ClusterManager) GetNextAttachableHost(currentHost string) string {
	if len(cm.AttachableHosts) == 0 {
		return ""
	}
	for i, host := range cm.AttachableHosts {
		if host == currentHost {
			return cm.AttachableHosts[(i+1)%len(cm.AttachableHosts)]
		}
	}
	return ""
}
