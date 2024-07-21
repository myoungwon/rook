package clustermanager

import "fmt"

// FabricOSDInfo contains information about an OSD that is attached to a node
type FabricOSDInfo struct {
	id       string
	address  string
	port     string
	subnqn   string
	hostname string
}

type FabricMap struct {
	osdsByNode map[string][]FabricOSDInfo
}

func NewOSDNodeMap() FabricMap {
	return FabricMap{
		osdsByNode: make(map[string][]FabricOSDInfo),
	}
}

// AddOSD adds an OSD to the map
func (o FabricMap) AddOSD(osdID, node, address, port, subnqn string) {
	o.osdsByNode[node] = append(o.osdsByNode[node], FabricOSDInfo{
		id:       osdID,
		address:  address,
		port:     port,
		subnqn:   subnqn,
		hostname: node,
	})
}

// RemoveOSD removes an OSD from the map
func (o *FabricMap) RemoveOSD(osdID, node string) {
	osds := o.osdsByNode[node]
	for i, osdInfo := range osds {
		if osdInfo.id == osdID {
			o.osdsByNode[node] = append(osds[:i], osds[i+1:]...)
			if len(o.osdsByNode[node]) == 0 {
				delete(o.osdsByNode, node)
			}
			break
		}
	}
}

// FindNodeByOSD finds the node that an OSD is attached to
func (o *FabricMap) FindNodeByOSD(osdID string) (string, error) {
	for node, osds := range o.osdsByNode {
		for _, osdInfo := range osds {
			if osdInfo.id == osdID {
				return node, nil
			}
		}
	}
	return "", fmt.Errorf("OSD %s is not attached to any node", osdID)
}

// FindOSDBySubNQN finds the OSDInfo that has a given subnqn
func (o *FabricMap) FindOSDBySubNQN(subnqn string) (FabricOSDInfo, error) {
	for _, osds := range o.osdsByNode {
		for _, osdInfo := range osds {
			if osdInfo.subnqn == subnqn {
				return osdInfo, nil
			}
		}
	}
	return FabricOSDInfo{}, fmt.Errorf("OSD not found for subnqn %s", subnqn)
}

// FindOSDsByNode returns the OSDs that are attached to a node
func (o *FabricMap) FindOSDsByNode(node string) ([]FabricOSDInfo, bool) {
	osds, ok := o.osdsByNode[node]
	return osds, ok
}

// GetNodes returns the attachable nodes
func (o *FabricMap) GetNodes() []string {
	nodes := make([]string, 0, len(o.osdsByNode))
	for node := range o.osdsByNode {
		nodes = append(nodes, node)
	}
	return nodes
}
