package latencyaware

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	pluginConfig "sigs.k8s.io/scheduler-plugins/apis/config"
)

const (
	Name         = "LatencyAware"
	DefaultScore = 0
)

// Name : returns name of the plugin
func (la *LatencyAware) Name() string {
	return Name
}

type LatencyAware struct {
	sync.Mutex
	handle                     framework.Handle
	informer                   cache.SharedInformer
	reservedProbeNodes         VisitedNodes //reservedNodes non è funzionante perché i pod target e probe potrebbero essere schedulati sullo stesso nodo
	reservedTargetNodes        VisitedNodes
	nodesWithTargetAndNotProbe VisitedNodes
	nodesWithProbeAndNotTarget VisitedNodes //TODO: se il deployment viene eliminato e questa struttura ha una entry questa non verrà mai eliminata -> è necessario essere in ascolto del descheduling per ripulire la struttura
	probeVisitedNodes          VisitedNodes
}

var _ framework.FilterPlugin = &LatencyAware{}
var _ framework.ScorePlugin = &LatencyAware{}
var _ framework.PostBindPlugin = &LatencyAware{}

// Filter : filter nodes based on pod label
func (la *LatencyAware) Filter(ctx context.Context,
	cycleState *framework.CycleState,
	pod *corev1.Pod,
	nodeInfo *framework.NodeInfo) *framework.Status {

	la.Lock()
	defer la.Unlock()

	// Check if the pod is not "probe" or "target"
	if pod.Labels["app"] != ProbeAppLabel && pod.Labels["app"] != TargetAppLabel {
		return framework.NewStatus(framework.Success,
			fmt.Sprintf("Pod %v is not \"probe\" or \"target\"", pod.Name))
	}

	if data, err := cycleState.Read("ReservedNode"); err == nil {
		klog.Infof("[LatencyAware] Pod %v already has a reserved node: %v", pod.Name, data.(*ReservedNodeState).nodeName)

		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("Pod %v already has a reserved node: %v", pod.Name, nodeInfo.Node().Name))
	}

	// Your code to get the pod "app" label goes here
	appLabel := pod.Labels["app"]
	klog.Infof("[LatencyAware] Pod %v has app label: %v (in node %v)\n", pod.Name, appLabel, nodeInfo.Node().Name)

	if appLabel == TargetAppLabel { // Check if the pod is "probe"
		// Check if the node has currently a "probe" pod
		/*if la.currentTargetNodes.IsVisited(nodeInfo.Node().Name) {
			klog.Infof("[LatencyAware] Node %v already has \"target\"", nodeInfo.Node().Name)

			return framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("Node %v is already been visited by a \"probe\" pod", nodeInfo.Node().Name))
		}*/

		if hasPodWithLabel(nodeInfo.Pods, TargetAppLabel) {
			klog.Infof("[LatencyAware] Node %v already has \"target\"", nodeInfo.Node().Name)

			return framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("Node %v is already been visited by a \"probe\" pod", nodeInfo.Node().Name))
		}

		if len(la.nodesWithProbeAndNotTarget.nodes) > 0 {
			klog.Infof("[LatencyAware] There are some nodes with \"probe\" and not \"target\"")

			if !la.nodesWithProbeAndNotTarget.IsVisited(nodeInfo.Node().Name) {
				klog.Infof("[LatencyAware] Node %v is invalid for \"target\" because other nodes have \"probe\" without \"target\"", nodeInfo.Node().Name)

				return framework.NewStatus(framework.Unschedulable,
					fmt.Sprintf("Node %v is invalid for \"target\" because other nodes have \"probe\" without \"target\"", nodeInfo.Node().Name))
			}
		}

		//problema: nodesWithProbeAndNotTarget funziona solo quando i pod sono già schedulati; in caso siano ancora in pending non funziona
		//			va calcolata la differenza di reservedTargetNodes e reservedProbeNodes

		if diff := la.reservedProbeNodes.Difference(&la.reservedTargetNodes); len(diff.nodes) > 0 {
			if !diff.IsVisited(nodeInfo.Node().Name) {
				klog.Infof("[LatencyAware] Node %v is invalid for \"target\" because there are some reserved nodes for \"probe\" and this is not one of them", nodeInfo.Node().Name)

				return framework.NewStatus(framework.Unschedulable,
					fmt.Sprintf("Node %v is invalid for \"target\" because other nodes are reserved for \"probe\" and not \"target\"", nodeInfo.Node().Name))
			}
		}

		if la.reservedTargetNodes.SetVisitedIfNot(nodeInfo.Node().Name) {
			klog.Infof("[LatencyAware] Node %v is valid for %v", nodeInfo.Node().Name, pod.Name)

			cycleState.Write("ReservedNode", &ReservedNodeState{
				nodeName: nodeInfo.Node().Name,
			})

			return framework.NewStatus(framework.Success,
				fmt.Sprintf("Pod %v can be scheduled in %v", pod.Name, nodeInfo.Node().Name))
		}
	} else if appLabel == ProbeAppLabel { // Check if the pod is "target"
		// Check if the node is already visited by a "target" pod
		if la.probeVisitedNodes.IsVisited(nodeInfo.Node().Name) {
			klog.Infof("[LatencyAware] Node %v already had (has) \"probe\"", nodeInfo.Node().Name)

			return framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("Node %v is already been visited by a \"target\" pod", nodeInfo.Node().Name))
		}

		if len(la.nodesWithTargetAndNotProbe.nodes) > 0 {
			klog.Infof("[LatencyAware] There are some nodes with \"target\" and not \"probe\"")

			if !la.nodesWithTargetAndNotProbe.IsVisited(nodeInfo.Node().Name) {
				klog.Infof("[LatencyAware] Node %v is invalid for \"probe\" because other nodes have \"target\" without \"probe\"", nodeInfo.Node().Name)

				return framework.NewStatus(framework.Unschedulable,
					fmt.Sprintf("Node %v is invalid for \"probe\" because other nodes have \"target\" without \"probe\"", nodeInfo.Node().Name))
			}
		}

		if diff := la.reservedTargetNodes.Difference(&la.reservedProbeNodes); len(diff.nodes) > 0 {
			if !diff.IsVisited(nodeInfo.Node().Name) {
				klog.Infof("[LatencyAware] Node %v is invalid for \"probe\" because there are some reserved nodes for \"target\" and this is not one of them", nodeInfo.Node().Name)

				return framework.NewStatus(framework.Unschedulable,
					fmt.Sprintf("Node %v is invalid for \"target\" because other nodes are reserved for \"target\" and not \"probe\"", nodeInfo.Node().Name))
			}
		}

		if la.reservedProbeNodes.SetVisitedIfNot(nodeInfo.Node().Name) {
			klog.Infof("[LatencyAware] Node %v is valid for %v", nodeInfo.Node().Name, pod.Name)

			cycleState.Write("ReservedNode", &ReservedNodeState{
				nodeName: nodeInfo.Node().Name,
			})

			return framework.NewStatus(framework.Success,
				fmt.Sprintf("Pod %v can be scheduled in %v", pod.Name, nodeInfo.Node().Name))
		}
	}

	//PROBLEMA: due diversi nodi per lo stesso pod potrebbero arrivare contemporaneamente qui ed essere occupati entrambi
	//  	  bisogna fare in modo che solo uno dei due possa riservare il nodo (lock dell'intero filter)

	klog.Infof("[LatencyAware] Node %v is ALREADY RESERVED for %v", nodeInfo.Node().Name, appLabel)

	return framework.NewStatus(framework.Unschedulable,
		fmt.Sprintf("Node %v is ALREADY RESERVED for %v", nodeInfo.Node().Name, appLabel))
}

func hasPodWithLabel(pods interface{}, label string) bool {
	switch p := pods.(type) {
	case []*framework.PodInfo:
		// Handle the case where the input is a slice of *framework.PodInfo
		for _, podInfo := range p {
			if pod := podInfo.Pod; pod != nil {
				if val, exists := pod.Labels["app"]; exists && val == label {
					return true
				}
			}
		}
	case *corev1.PodList:
		// Handle the case where the input is a *corev1.PodList
		for _, pod := range p.Items {
			if val, exists := pod.Labels["app"]; exists && val == label {
				return true
			}
		}
	default:
		// Handle unexpected type
		return false
	}
	return false
}

// ScoreExtensions : an interface for Score extended functionality
func (la *LatencyAware) ScoreExtensions() framework.ScoreExtensions {
	return la
}

func (no *LatencyAware) NormalizeScore(ctx context.Context,
	state *framework.CycleState,
	pod *corev1.Pod,
	scores framework.NodeScoreList) *framework.Status {
	return nil
}

// Score : evaluate score for a node
func (la *LatencyAware) Score(ctx context.Context,
	cycleState *framework.CycleState,
	pod *corev1.Pod,
	nodeName string) (int64, *framework.Status) {

	score := int64(0)
	klog.Infof("[LatencyAware] Score for node %s is %d", nodeName, score)

	return DefaultScore, nil
}

// ScoreExtensions : an interface for Score extended functionality
/*func (la *LatencyAware) ScoreExtensions() framework {
	return la
}*/

func (la *LatencyAware) PostBind(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) {
	klog.Infof("[LatencyAware] PostBind: %v\n", pod.Labels["app"])

	// Check if the pod is not "probe" or "target"
	if pod.Labels["app"] != ProbeAppLabel && pod.Labels["app"] != TargetAppLabel {
		return
	}

	// Get the Kubernetes client from the framework handle
	client := la.handle.ClientSet()

	// Fetch the list of nodes
	nodes, err := client.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})

	// Fetch the list of pods on the same node
	pods, err := client.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	})

	if err != nil {
		// Handle the error appropriately
		klog.Errorf("[LatencyAware] Error listing pods on node %s: %v", nodeName, err)
		return
	}

	// Your code to get the pod "app" label goes here
	appLabel := pod.Labels["app"]
	klog.Infof("[LatencyAware] Pod %v has been scheduled in: %v\n", pod.Name, nodeName)

	if appLabel == TargetAppLabel { // Check if the pod is "probe"
		// Check if the node has currently a "probe" pod -> if not inserts the node in the set of nodes with target and not probe
		if !hasPodWithLabel(pods, ProbeAppLabel) {
			la.nodesWithTargetAndNotProbe.SetVisited(nodeName)
		} else {
			la.nodesWithProbeAndNotTarget.SetUnvisited(nodeName)
		}

		if la.reservedTargetNodes.SetUnvisited(nodeName) {
			klog.Infof("[LatencyAware] Node %v is not reserved anymore", nodeName)
		}
	} else if appLabel == ProbeAppLabel { // Check if the pod is "target"
		// Check if the node has currently a "target" pod -> if not inserts the node in the set of nodes with probe and not target
		if !hasPodWithLabel(pods, TargetAppLabel) {
			la.nodesWithProbeAndNotTarget.SetVisited(nodeName)
		} else {
			la.nodesWithTargetAndNotProbe.SetUnvisited(nodeName)
		}

		// Set the node as visited by a "probe" pod
		la.probeVisitedNodes.SetVisited(nodeName)

		if len(la.probeVisitedNodes.nodes) == len(nodes.Items) {
			la.probeVisitedNodes.Reset()
		}

		if la.reservedProbeNodes.SetUnvisited(nodeName) {
			klog.Infof("[LatencyAware] Node %v is not reserved anymore", nodeName)
		}
	}

	klog.Infof("[LatencyAware] ----------------------- reservedProbeNodes:         %v", la.reservedProbeNodes.Join())
	klog.Infof("[LatencyAware] ----------------------- reservedTargetNodes:        %v", la.reservedTargetNodes.Join())
	klog.Infof("[LatencyAware] ----------------------- nodesWithTargetAndNotProbe: %v", la.nodesWithTargetAndNotProbe.Join())
	klog.Infof("[LatencyAware] ----------------------- nodesWithProbeAndNotTarget: %v", la.nodesWithProbeAndNotTarget.Join())
	klog.Infof("[LatencyAware] ----------------------- probeVisitedNodes:          %v", la.probeVisitedNodes.Join())
}

var ProbeAppLabel string
var TargetAppLabel string

/*type VisitedNodes struct {
	probeNodes  map[string]bool
	targetNodes map[string]bool
}

func (la *LatencyAware) Schedule(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	visited := state.GetVisitedNodes()
	if visited == nil {
		visited = &VisitedNodes{
			probeNodes:  make(map[string]bool),
			targetNodes: make(map[string]bool),
		}
		state.SetVisitedNodes(visited)
	}

	if pod.Labels[ProbeAppLabel] != "" {
		visited.probeNodes[nodeName] = true
	} else if pod.Labels[TargetAppLabel] != "" {
		visited.targetNodes[nodeName] = true
	}

	return nil
}*/

// OnNodeAdd is called when a new node is added
func (la *LatencyAware) OnNodeAdd(node *corev1.Node) {
	fmt.Printf("Node added: %s\n", node.Name)
}

// OnNodeUpdate is called when a node is updated
func (la *LatencyAware) OnNodeUpdate(oldNode, newNode *corev1.Node) {
	fmt.Printf("Node updated: %s\n", newNode.Name)
}

// OnNodeDelete is called when a node is deleted
func (la *LatencyAware) OnNodeDelete(node *corev1.Node) {
	fmt.Printf("Node deleted: %s\n", node.Name)
}

// New initializes a new plugin and returns it.
func New(_ context.Context, obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	var args, ok = obj.(*pluginConfig.LatencyAwareArgs)
	if !ok {
		return nil, fmt.Errorf("[LatencyAwareArgs] want args to be of type LatencyAwareArgs, got %T", obj)
	}

	klog.Infof("[LatencyAwareArgs] args received. ProbeAppLabel: %s, TargetAppLabel: %s", args.ProbeAppLabel, args.TargetAppLabel)
	ProbeAppLabel = args.ProbeAppLabel
	TargetAppLabel = args.TargetAppLabel

	la := &LatencyAware{
		handle: h,
		reservedProbeNodes: VisitedNodes{
			mu:    sync.Mutex{},
			nodes: make([]string, 0),
		},
		reservedTargetNodes: VisitedNodes{
			mu:    sync.Mutex{},
			nodes: make([]string, 0),
		},
		nodesWithProbeAndNotTarget: VisitedNodes{
			mu:    sync.Mutex{},
			nodes: make([]string, 0),
		},
		nodesWithTargetAndNotProbe: VisitedNodes{
			mu:    sync.Mutex{},
			nodes: make([]string, 0),
		},
		probeVisitedNodes: VisitedNodes{
			mu:    sync.Mutex{},
			nodes: make([]string, 0),
		},
	}

	la.informer = h.SharedInformerFactory().Core().V1().Pods().Informer()

	la.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				fmt.Println("Error casting to pod")
				return
			}

			appLabel := pod.Labels["app"]

			if appLabel == ProbeAppLabel {
				if la.nodesWithProbeAndNotTarget.SetUnvisited(pod.Spec.NodeName) {
					klog.Infof("[LatencyAware] Node %v had only \"probe\": deleted %v\n", pod.Spec.NodeName, pod.Name)
				}
			} else if appLabel == TargetAppLabel {
				if la.nodesWithTargetAndNotProbe.SetUnvisited(pod.Spec.NodeName) {
					klog.Infof("[LatencyAware] Node %v had only \"target\": deleted %v\n", pod.Spec.NodeName, pod.Name)
				}
			}
		},
	})

	go la.informer.Run(context.Background().Done())

	return la, nil
}

/*
	TODO:
	1. inizialmente lo scheduler non ha nessun nodo visitato
	2. bisognerà schedulare sia i "probe" che i "target"
	3. non si sa con quale ordine avverrà la schedulazione (prima "probe" o "target") quindi bisognerà fare un controllo
	4. se uno dei due è stato già schedulato bisognerà schedulare l'altro sullo stesso nodo (attenzione le repliche "probe" solitamente sono "target" - 1)
	5. bisogna tenere traccia dei nodi visitati "probe" e "target": la distinzione è necessaria perché
		- se inizialmente un nodo è visitato solo da un "probe" può comunque essere visitato da un "target" (ad esempio se si scopre che questo nodo è migliore dei nodi attuali di "target")
		- MA non può essere visitato da un "probe" nuovamente

*/
