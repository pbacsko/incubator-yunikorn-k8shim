package predicates

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

var enabled bool
var skipFunc map[string]func(*v1.Pod, *framework.NodeInfo) bool

func init() {
	skipFunc = make(map[string]func(*v1.Pod, *framework.NodeInfo) bool)

	skipFunc[names.PodTopologySpread] = func(pod *v1.Pod, _ *framework.NodeInfo) bool {
		return len(pod.Spec.TopologySpreadConstraints) == 0
	}
	skipFunc[names.TaintToleration] = func(_ *v1.Pod, node *framework.NodeInfo) bool {
		return len(node.Node().Spec.Taints) == 0
	}
	skipFunc[names.NodeAffinity] = func(pod *v1.Pod, _ *framework.NodeInfo) bool {
		skipAffinity := false
		if pod.Spec.Affinity == nil {
			skipAffinity = true
		} else if pod.Spec.Affinity.NodeAffinity == nil {
			skipAffinity = true
		} else if pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
			skipAffinity = true
		}

		return len(pod.Spec.NodeSelector) == 0 && skipAffinity
	}
}

func CanSkipPredicate(name string, pod *v1.Pod, nodeInfo *framework.NodeInfo) bool {
	if canSkip, ok := skipFunc[name]; ok && enabled {
		return canSkip(pod, nodeInfo)
	}

	return false
}
