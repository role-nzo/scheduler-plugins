package latencyaware

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	pluginConfig "sigs.k8s.io/scheduler-plugins/apis/config"
)

const (
	Name                      = "LatencyAware"
	DefaultScore              = 0
	targetOnProbeNodeStateKey = "targetOnProbeNode"
	featureLabel              = "latency-aware-deployment"
)

type TargetOnProbeNodeData struct {
	value bool
}

func (t *TargetOnProbeNodeData) Clone() framework.StateData {
	return &TargetOnProbeNodeData{
		value: t.value,
	}
}

// Name : returns name of the plugin
func (la *LatencyAware) Name() string {
	return Name
}

type LatencyAware struct {
	sync.Mutex
	cancelTimeout              *func()
	handle                     framework.Handle
	informer                   cache.SharedInformer
	nodesWithProbeAndNotTarget VisitedNodes
	probeVisitedNodes          VisitedNodes
}

var _ framework.FilterPlugin = &LatencyAware{}
var _ framework.PostBindPlugin = &LatencyAware{}
var _ framework.ReservePlugin = &LatencyAware{}
var _ framework.PermitPlugin = &LatencyAware{}

func (la *LatencyAware) getNodeCount() (int, error) {
	nodes, err := la.handle.ClientSet().CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	return len(nodes.Items), nil
}

func (la *LatencyAware) getPodsByLabelSelector(ctx context.Context, labelSelector string) (*corev1.PodList, error) {
	podList, err := la.handle.ClientSet().CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, err
	}

	return podList, nil
}

func (la *LatencyAware) getPodsByAppLabel(ctx context.Context, label string) (*corev1.PodList, error) {
	return la.getPodsByLabelSelector(ctx, labels.SelectorFromSet(labels.Set{
		"app": label,
	}).String())
}

func (la *LatencyAware) getPodsByFeatureLabel(ctx context.Context, feature string) (*corev1.PodList, error) {
	return la.getPodsByLabelSelector(ctx, labels.SelectorFromSet(labels.Set{
		"feature": feature,
	}).String())
}

// Filter : filter nodes based on pod label
func (la *LatencyAware) Filter(ctx context.Context,
	cycleState *framework.CycleState,
	pod *corev1.Pod,
	nodeInfo *framework.NodeInfo) *framework.Status {

	appLabel := pod.Labels["app"]

	// If the pod is neither a probe nor a target, it can be scheduled anywhere
	if appLabel != ProbeAppLabel && appLabel != TargetAppLabel {
		return framework.NewStatus(framework.Success,
			fmt.Sprintf("Pod %v can be scheduled in %v", pod.Name, nodeInfo.Node().Name))
	}

	// If
	//  - the pod is a target
	//  - (DISABLED) all the nodes have been visited by the probe or target (no completely free nodes)
	//  - there are nodes with a probe and not a target
	// then the pod can be scheduled only in the nodes with a probe and not a target
	//		because they are (after the initial transient phase) better nodes than the one visited by the target pod descheduled
	if appLabel == TargetAppLabel && len(la.nodesWithProbeAndNotTarget.nodes) > 0 {
		if la.nodesWithProbeAndNotTarget.IsVisited(nodeInfo.Node().Name) {
			klog.Infof("[LatencyAware] Pod %v CAN be scheduled in \"probe\" only node: %v", pod.Name, nodeInfo.Node().Name)
			return framework.NewStatus(framework.Success,
				fmt.Sprintf("Pod %v can be scheduled in %v", pod.Name, nodeInfo.Node().Name))
		} else {
			klog.Infof("[LatencyAware] Pod %v CANNOT be scheduled in the node %v because there are nodes with only probe", pod.Name, nodeInfo.Node().Name)
			return framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("Pod %v cannot be scheduled in %v", pod.Name, nodeInfo.Node().Name))
		}
	}

	// If the node is not yet visited by the probe pod
	if !la.probeVisitedNodes.IsVisited(nodeInfo.Node().Name) {
		klog.Infof("[LatencyAware] Pod %v CAN be scheduled in: %v", pod.Name, nodeInfo.Node().Name)
		return framework.NewStatus(framework.Success,
			fmt.Sprintf("Pod %v can be scheduled in %v", pod.Name, nodeInfo.Node().Name))
	}

	// If the node is already visited by the probe pod it cannot be scheduled
	klog.Infof("[LatencyAware] Pod %v CANNOT be scheduled in: %v", pod.Name, nodeInfo.Node().Name)
	return framework.NewStatus(framework.Unschedulable,
		fmt.Sprintf("Pod %v cannot be scheduled in %v", pod.Name, nodeInfo.Node().Name))
}

// Reserve: reserve the node chosen for the pod
// If the node is already visited, the pod cannot be scheduled (but this should never happen, see the body function)
// If this function (or any subsequent plugin in this or subsequent stages) returns an error, the node will be unvisited in the "Unreserve" function
func (la *LatencyAware) Reserve(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) *framework.Status {
	appLabel := pod.Labels["app"]

	klog.Infof("[LatencyAware] RESERVE: Pod %v visited %v", pod.Name, nodeName)

	// If the pod is neither a probe nor a target, it can be scheduled anywhere
	if appLabel != ProbeAppLabel && appLabel != TargetAppLabel {
		return framework.NewStatus(framework.Success,
			fmt.Sprintf("Pod %v can be scheduled in %v", pod.Name, nodeName))
	}

	// If the node is not yet visited by the pod
	if la.probeVisitedNodes.SetVisitedIfNot(nodeName) {
		klog.Infof("[LatencyAware] Pod %v visited %v", pod.Name, nodeName)

		// If it's a probe only pod, the node is inserted in the list of nodes with a probe and not a target
		if appLabel == ProbeAppLabel {
			klog.Infof("[LatencyAware] Probe pod %v reserved %v", pod.Name, nodeName)
			la.nodesWithProbeAndNotTarget.SetVisited(nodeName)
		}

		return framework.NewStatus(framework.Success)
	}

	// If the pod is a target
	if appLabel == TargetAppLabel {
		// If the node was visited by a probe pod and not a target pod
		if la.nodesWithProbeAndNotTarget.SetUnvisited(nodeName) {
			// code here is executed only if the node was in the list of nodes with a probe and not a target

			// Prevents that two or more targets get scheduled in the node
			//	only one target can be scheduled in the node visited by the probe
			// If this is not done, the filter function will allow the scheduling of the target pod
			state.Write(targetOnProbeNodeStateKey, &TargetOnProbeNodeData{value: true})
		}
	}

	// If the node is already visited by the pod
	//    this happens when trying to schedule a target pod on a node already visited by a probe pod
	return framework.NewStatus(framework.Success)

	// NOTE: this could be executed when scheduling a target over a node already visited by a probe

	// Toeretically this should never be executed
	//	the pod scheduling scheduling cycle is sequential and not concurrent
	//	only the filter plugin is executed concurrently for the same pod
	//  the nodes that are already visited never reach the permit plugin again
	//
	//  POD 1                                        POD 2
	// 	----------------------                       ----------------------
	//  Filter N1 -\               					 Filter N1 -\
	//  Filter N2 -- Reserve NX -- PostBind NX		 Filter N2 -- Reserve NY -- PostBind NY
	//  Filter N3 -/							     Filter N3 -/
	//
	//klog.Infof("[LatencyAware] Node %v IS ALREADY VISITED (pod %v)", nodeName, pod.Name)
	//return framework.NewStatus(framework.Unschedulable)
}

// Unreserve: executed if the "Reserve" or any subsequent plugin fails
// This is important because the state of the plugin is updated freeing the reserved node for the next scheduling cycle
// If this function is not implemented the node will remain reserved even if the scheduling cycle fails
func (la *LatencyAware) Unreserve(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) {
	appLabel := pod.Labels["app"]

	if appLabel != ProbeAppLabel && appLabel != TargetAppLabel {
		return
	}

	klog.Infof("[LatencyAware] UNRESERVE: Pod %v visited %v", pod.Name, nodeName)

	// Sets the node as unvisited
	la.probeVisitedNodes.SetUnvisited(nodeName)

	if appLabel == TargetAppLabel {
		// Retrieve the item from the CycleState
		raw, err := state.Read(targetOnProbeNodeStateKey)
		if err != nil {
			return
		}

		// Assert the type to your custom data type
		_, ok := raw.(*TargetOnProbeNodeData)
		if !ok {
			return
		}

		// If the node was replacing the probe node, resets the node as visited only by the probe
		la.nodesWithProbeAndNotTarget.SetVisited(nodeName)
	} else if appLabel == ProbeAppLabel {
		la.nodesWithProbeAndNotTarget.SetUnvisited(nodeName)
	}
}

// Permit: permit the scheduling of the pod on the node (useless for now)
func (la *LatencyAware) Permit(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) (*framework.Status, time.Duration) {

	appLabel := pod.Labels["app"]

	klog.Infof("[LatencyAware] PERMIT: Pod %v visited %v", pod.Name, nodeName)

	// If the pod is neither a probe nor a target, it can be scheduled anywhere
	if appLabel != ProbeAppLabel && appLabel != TargetAppLabel {
		return framework.NewStatus(framework.Success,
			fmt.Sprintf("Pod %v can be scheduled in %v", pod.Name, nodeName)), 0
	}

	return framework.NewStatus(framework.Success,
		fmt.Sprintf("Pod %v can be scheduled in %v", pod.Name, nodeName)), 0

	// random unschedulable: for testing
	/*if rand.Intn(2) == 0 {
		klog.Infof("[LatencyAware] Pod %v scheduled in: %v", pod.Name, nodeName)
		return framework.NewStatus(framework.Success), 0
	} else {
		klog.Infof("[LatencyAware] KILLING: Pod %v visited %v", pod.Name, nodeName)
		klog.Infof("[LatencyAware] Pod %v not scheduled in: %v", pod.Name, nodeName)
		return framework.NewStatus(framework.Unschedulable), 0
	}*/
}

func (la *LatencyAware) PostBind(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeName string) {
	klog.Infof("[LatencyAware] Pod %v has been scheduled in: %v (DT: %v)", pod.Name, nodeName, pod.DeletionTimestamp)

	appLabel := pod.Labels["app"]

	// If the pod is a target
	if appLabel == TargetAppLabel {
		// If the node was visited by a probe pod and not a target pod
		//	This can be done because the "Reserve" function is executed before the "PostBind" function
		//  the function inserts a state in the CycleState to signal that the target pod is replacing the probe pod

		// Retrieve the item from the CycleState
		raw, err := state.Read(targetOnProbeNodeStateKey)
		if err == nil {

			// Assert the type to your custom data type
			_, ok := raw.(*TargetOnProbeNodeData)
			if ok {
				// Find and deschedule the ProbeAppLabel pod on the same node

				state.Delete(targetOnProbeNodeStateKey)

				// Get the list of pods with the probe label (containing only probe)
				podList, err := la.handle.ClientSet().CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{
					LabelSelector: labels.SelectorFromSet(labels.Set{
						"app": ProbeAppLabel,
					}).String(),
				})
				if err != nil {
					klog.Errorf("[LatencyAware] Error listing pods: %v", err)
					return
				}

				// For each pod with the probe label
				for _, p := range podList.Items {
					// If the pod is scheduled on the same node
					if p.Spec.NodeName == nodeName {
						klog.Infof("[LatencyAware] Descheduling ProbeAppLabel pod %v on node %v", p.Name, nodeName)
						// Deschedule the probe pod on the same node
						err := la.handle.ClientSet().CoreV1().Pods(p.Namespace).Delete(context.TODO(), p.Name, metav1.DeleteOptions{})
						if err != nil {
							klog.Errorf("[LatencyAware] Error deleting pod: %v", err)
						}
					}
				}
			}
		}
	}

	// Start the timeout to reset the visited nodes
	la.startTimeout(ctx)
}

// handlePodDelete: handle the deletion of a pod (for now only probe pods)
func (la *LatencyAware) handlePodDelete(obj interface{}, ctx context.Context) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		fmt.Println("Error casting to pod")
		return
	}

	appLabel := pod.Labels["app"]

	// If the pod is probe
	if appLabel == ProbeAppLabel {
		// If the node is visited by ONLY the probe pod, remove it from the list
		if la.nodesWithProbeAndNotTarget.SetUnvisited(pod.Spec.NodeName) {
			klog.Infof("[LatencyAware] Probe pod %v left %v", pod.Name, pod.Spec.NodeName)

			// Start the timeout to reset the visited nodes
			la.startTimeout(ctx)
		}
	}

	podList, err := la.getPodsByFeatureLabel(ctx, featureLabel)
	if err != nil {
		klog.Errorf("[LatencyAware] Error listing pods: %v", err)
		return
	}

	klog.Infof("[LatencyAware] \"target\" or \"probe\" pod in deletion: %v", pod.Name)

	inDeletion := 0
	for _, pod := range podList.Items {
		if pod.DeletionTimestamp != nil {
			inDeletion++
			klog.Infof("[LatencyAware] Pod %v has a deletion timestamp: %v (state %v)", pod.Name, pod.DeletionTimestamp, pod.Status.Phase)
		} else {
			klog.Infof("[LatencyAware] Pod %v has NOT a deletion timestamp: %v (state %v)", pod.Name, pod.DeletionTimestamp, pod.Status.Phase)
		}
	}

	// If all the pods are in deletion, reset the visited nodes
	//	this happens when the deployment is deleted
	//	it's robust even if the deletion happens sequentially
	//
	// parallel example:
	// DEL POD 1 -\
	// DEL POD 2 -- 3 == inDeletion == len(podList.Items) == 3 -- true
	// DEL POD 3 -/
	//
	// sequential example:
	// DEL POD 1 -- 1 == inDeletion != len(podList.Items) == 3 -- false,
	//     after DEL POD 2 -- 1 == inDeletion != len(podList.Items) == 2 -- false,
	//         after DEL POD 3 -- 1 == inDeletion == len(podList.Items) == 1 -- true
	//
	if len(podList.Items) == inDeletion {
		klog.Infof("[LatencyAware] Resetting visited nodes because there are no more target pods")

		if la.cancelTimeout != nil {
			klog.Infof("[LatencyAware] A reset timer was set. Cancelling it...")
			(*la.cancelTimeout)()
		}

		la.resetAndFillStructs(ctx)
	}

}

// startTimeout: start the timeout to reset the visited nodes
// If all the nodes have been visited by the probe+target pods and there are no nodes with a probe and not a target
func (la *LatencyAware) startTimeout(ctx context.Context) error {
	// Get the number of nodes in the cluster
	numNodes, err := la.getNodeCount()
	if err != nil {
		klog.Errorf("[LatencyAware] Error getting the number of nodes: %v", err)
		return err
	}

	// stall condition: no actions can be taken anymore by the plugin
	// 		- all the nodes have been visited by the probe+target pods
	//		- there are no nodes with a probe and not a target
	if len(la.probeVisitedNodes.nodes) == numNodes && len(la.nodesWithProbeAndNotTarget.nodes) == 0 {
		klog.Infof("[LatencyAware] Timer set.")

		if la.cancelTimeout != nil {
			(*la.cancelTimeout)()
			la.cancelTimeout = nil
		}

		cancelContext, cancel := context.WithCancel(context.Background())
		cleanup := func() {
			cancel()
			klog.Infof("[LatencyAware] Timer cancelled.")
			la.cancelTimeout = nil
		}
		la.cancelTimeout = &cleanup

		go func() {
			time.Sleep(30 * time.Second)

			select {
			case <-cancelContext.Done():
				klog.Infof("[LatencyAware] Reset goroutine cancelled.")
				return
			default:
				if la.cancelTimeout == &cleanup {
					la.cancelTimeout = nil
				}

				klog.Infof("[LatencyAware] Resetting visited nodes...")

				la.resetAndFillStructs(ctx)

				klog.Infof("[LatencyAware] Resetting visited nodes completed: \n\t\tprobeVisitedNodes:\t%v\n\t\tnodesWithProbeAndNotTarget:\t%v", la.probeVisitedNodes.Join(), la.nodesWithProbeAndNotTarget.Join())
			}
		}()
	}

	return nil
}

var ProbeAppLabel string
var TargetAppLabel string

// New initializes a new plugin and returns it.
func New(ctx context.Context, obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	var args, ok = obj.(*pluginConfig.LatencyAwareArgs)
	if !ok {
		return nil, fmt.Errorf("[LatencyAwareArgs] want args to be of type LatencyAwareArgs, got %T", obj)
	}

	klog.Infof("[LatencyAwareArgs] args received. ProbeAppLabel: %s, TargetAppLabel: %s", args.ProbeAppLabel, args.TargetAppLabel)
	ProbeAppLabel = args.ProbeAppLabel
	TargetAppLabel = args.TargetAppLabel

	la := &LatencyAware{
		handle:        h,
		cancelTimeout: nil,
		nodesWithProbeAndNotTarget: VisitedNodes{
			mu:    sync.Mutex{},
			nodes: make([]string, 0),
		},
		probeVisitedNodes: VisitedNodes{
			mu:    sync.Mutex{},
			nodes: make([]string, 0),
		},
	}

	// To reset the state of the plugin
	la.resetAndFillStructs(ctx)

	la.informer = h.SharedInformerFactory().Core().V1().Pods().Informer()

	la.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			la.handlePodDelete(obj, ctx)
		},
	})

	go la.informer.Run(context.Background().Done())

	return la, nil
}

// Reset and fill the structs with the nodes that have a probe or target pod
func (la *LatencyAware) resetAndFillStructs(ctx context.Context) error {
	// Get the list of pods with the target label (containing target and probe)
	targetPodList, errTarget := la.getPodsByAppLabel(ctx, TargetAppLabel)
	if errTarget != nil {
		klog.Errorf("[LatencyAware] Error listing target pods: %v", errTarget)
		return errTarget
	}

	// Get the list of pods with the probe label (containing only probe)
	probePodList, errProbe := la.getPodsByAppLabel(ctx, ProbeAppLabel)
	if errProbe != nil {
		klog.Errorf("[LatencyAware] Error listing probe pods: %v", errProbe)
		return errProbe
	}

	// Lock the structs
	la.probeVisitedNodes.mu.Lock()
	la.nodesWithProbeAndNotTarget.mu.Lock()
	defer la.probeVisitedNodes.mu.Unlock()
	defer la.nodesWithProbeAndNotTarget.mu.Unlock()

	// Reset the structs
	la.probeVisitedNodes.ResetWithoutLock()
	la.nodesWithProbeAndNotTarget.ResetWithoutLock()

	// Fill probeVisitedNodes with the nodes that have a target pod
	for _, pod := range targetPodList.Items {
		// If NodeName is not empty (the pod is scheduled)
		if pod.Spec.NodeName != "" {
			if pod.DeletionTimestamp != nil && (pod.Status.Phase == corev1.PodPending || pod.Status.Phase == corev1.PodRunning) {
				klog.Infof("[LatencyAware] Resetting probeVisitedNodes: %v with pod %v", pod.Spec.NodeName, pod.Name)
				la.probeVisitedNodes.SetVisitedWithoutLock(pod.Spec.NodeName)
			}
		}
	}

	// Fill nodesWithProbeAndNotTarget and probeVisitedNodes with the nodes that have a probe pod
	for _, pod := range probePodList.Items {
		// If NodeName is not empty (the pod is scheduled)
		if pod.Spec.NodeName != "" {
			if pod.DeletionTimestamp != nil && (pod.Status.Phase == corev1.PodPending || pod.Status.Phase == corev1.PodRunning) {
				klog.Infof("[LatencyAware] Resetting nodesWithProbeAndNotTarget: %v with pod %v", pod.Spec.NodeName, pod.Name)
				la.nodesWithProbeAndNotTarget.SetVisitedWithoutLock(pod.Spec.NodeName)
				la.probeVisitedNodes.SetVisitedWithoutLock(pod.Spec.NodeName)
			}
		}
	}

	return nil
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

/*
	TODO (fixed):
	quando l'intero deployment viene deschedulato il nodo in cui risiedeva "lm-server" (solo probe) non viene liberato da "probeVisitedNodes"
	succede quando avviene lm-server viene eliminato per ultimo:
		in questo caso il nodo non viene mai liberato perché resetAndFillStructs viene chiamato dopo l'ultimo nodo target eliminato
		nonostante lm-server sia ancora in esecuzione - il reset avviene troppo in anticipo

	SOLUZIONE: vedere se TUTTI i pod con "feature: latency-aware-deployment" sono in fase di eliminazione
*/

/*
	TODO (fixed):
	quando si schedula un pod "target" ed esiste già un pod "probe" questo potrebbe significare due cose
	- ci si trova nella fase iniziale in cui i pod vengono schedulati in modo casuale
	- ci si trova a regime in cui se un pod "target" deve essere schedulato è perché è stato appena deschedulato e deve essere rischedulato
		- in questo caso quindi "probe" era più efficiente di "target" e si potrebbe pensare di schedulare "target" direttamente sul nodo in cui si trova "probe"
		- in questo modo si ottiene sicuramente un vantaggio invece di schedulare "target" in un nodo diverso le cui prestazioni non sono state ancora testate
*/
