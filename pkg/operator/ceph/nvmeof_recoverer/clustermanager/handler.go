package clustermanager

import (
	"github.com/coreos/pkg/capnslog"
)

var logger = capnslog.NewPackageLogger("github.com/rook/rook", "cluster-manager")

type ClusterManager struct {
	HostExists      map[string]bool
	AttachableHosts []string
}

func New() *ClusterManager {
	return &ClusterManager{
		HostExists:      make(map[string]bool),
		AttachableHosts: []string{},
	}
}

func (cm *ClusterManager) AddAttachbleHost(hostname string) error {
	if !cm.HostExists[hostname] {
		cm.AttachableHosts = append(cm.AttachableHosts, hostname)
		cm.HostExists[hostname] = true
	}

	return nil
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
