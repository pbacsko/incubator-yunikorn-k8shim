package shim

import (
	"fmt"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/log"
)

func TestYK2536repro(t *testing.T) {
	log.UpdateLoggingConfig(map[string]string{
		"log.level": "DEBUG",
	})

	cluster := &MockScheduler{}
	cluster.init()

	pods := getTestPods(12, 1000, "root.a")
	i := 0
	for _, pod := range pods {
		pod.Spec.NodeName = fmt.Sprintf("node-%d", i)
		cluster.AddPodToLister(pod)
		i++
		i = i % 700
	}

	for i := 0; i < 700; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		node := &v1.Node{
			Spec: v1.NodeSpec{
				Unschedulable: false,
			},
			ObjectMeta: metav1.ObjectMeta{

				Name: nodeName,
				UID:  types.UID("UUID-" + nodeName),
			},
			Status: v1.NodeStatus{
				Conditions: []v1.NodeCondition{
					{Type: v1.NodeReady, Status: v1.ConditionTrue},
				},
				Allocatable: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewMilliQuantity(nodeCpuMilli, resource.DecimalSI),
					v1.ResourceMemory: *resource.NewScaledQuantity(nodeMemGiB, resource.Giga),
					v1.ResourcePods:   *resource.NewScaledQuantity(nodeNumPods, resource.Scale(0)),
				},
			},
		}
		cluster.AddNodeToLister(node)
	}

	assert.NilError(t, cluster.start(), "failed to initialize cluster")
	defer cluster.stop()
	// update config
	err := cluster.updateConfig(queueConfig, map[string]string{
		"log.level": "DEBUG",
	})
	assert.NilError(t, err, "update config failed")

	time.Sleep(time.Second * 5)

	for i := 0; i < 100; i++ {
		cluster.DeletePod(pods[i+500])
		time.Sleep(time.Millisecond * 200)
	}

	time.Sleep(time.Hour)
}
