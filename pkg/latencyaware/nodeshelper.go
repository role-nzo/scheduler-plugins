package latencyaware

import (
	"strings"
	"sync"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type VisitedNodes struct {
	mu    sync.Mutex
	nodes []string
}

func (vn *VisitedNodes) SetVisitedIfNot(nodeName string) bool {
	vn.mu.Lock()
	defer vn.mu.Unlock()

	// Find the index of the item
	for _, v := range vn.nodes {
		if v == nodeName {
			return false
		}
	}

	vn.nodes = append(vn.nodes, nodeName)

	return true
}

func (vn *VisitedNodes) SetVisited(nodeName string) {
	vn.mu.Lock()
	defer vn.mu.Unlock()
	vn.nodes = append(vn.nodes, nodeName)
}

func (vn *VisitedNodes) SetUnvisited(nodeName string) bool {
	vn.mu.Lock()
	defer vn.mu.Unlock()

	// Find the index of the item to be deleted
	index := -1
	for i, v := range vn.nodes {
		if v == nodeName {
			index = i
			break
		}
	}

	// If the item is found, remove it by slicing
	if index != -1 {
		vn.nodes = append(vn.nodes[:index], vn.nodes[index+1:]...)
		return true
	}

	return false
}

func (vn *VisitedNodes) IsVisited(nodeName string) bool {
	vn.mu.Lock()
	defer vn.mu.Unlock()
	for _, v := range vn.nodes {
		if v == nodeName {
			return true
		}
	}
	return false
}

func (vn *VisitedNodes) Reset() {
	vn.mu.Lock()
	defer vn.mu.Unlock()
	vn.nodes = make([]string, 0)
}

func (vn *VisitedNodes) Join() string {
	return strings.Join(vn.nodes, ", ")
}

func (vn1 *VisitedNodes) Difference(vn2 *VisitedNodes) *VisitedNodes {
	vn1.mu.Lock()
	vn2.mu.Lock()
	defer vn1.mu.Unlock()
	defer vn2.mu.Unlock()

	diff := make([]string, 0)

	for _, v1 := range vn1.nodes {
		found := false
		for _, v2 := range vn2.nodes {
			if v1 == v2 {
				found = true
				break
			}
		}
		if !found {
			diff = append(diff, v1)
		}
	}

	return &VisitedNodes{
		mu:    sync.Mutex{},
		nodes: diff,
	}
}

type ReservedNodeState struct {
	nodeName string
}

// Clone the ElasticQuotaSnapshot state.
func (s *ReservedNodeState) Clone() framework.StateData {
	return &ReservedNodeState{
		nodeName: strings.Clone(s.nodeName),
	}
}
