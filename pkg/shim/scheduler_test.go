/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package shim

import (
	"fmt"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apis "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestApplicationScheduling(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100000000
                vcore: 10
              max:
                memory: 150000000
                vcore: 20
`
	// init and register scheduler
	cluster := MockScheduler{}
	cluster.init()
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err, "update config failed")
	nodeLabels := map[string]string{
		"label1": "key1",
		"label2": "key2",
	}

	// register nodes
	err = cluster.addNode("test.host.01", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err, "add node failed")
	err = cluster.addNode("test.host.02", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err, "add node failed")

	// create app and tasks
	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 10000000).
		AddResource(siCommon.CPU, 1).
		Build()

	task1 := createTestPod("root.a", "app0001", "task0001", taskResource)
	task2 := createTestPod("root.a", "app0001", "task0002", taskResource)

	cluster.AddPod(task1)
	cluster.AddPod(task2)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, "app0001", cache.ApplicationStates().Running)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", cache.TaskStates().Bound)
	cluster.waitAndAssertTaskState(t, "app0001", "task0002", cache.TaskStates().Bound)

	// complete pods
	task1Upd := task1.DeepCopy()
	task1Upd.Status.Phase = v1.PodSucceeded
	cluster.UpdatePod(task1, task1Upd)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", cache.TaskStates().Completed)
	cluster.waitAndAssertApplicationState(t, "app0001", cache.ApplicationStates().Running)
	task2Upd := task2.DeepCopy()
	task2Upd.Status.Phase = v1.PodSucceeded
	cluster.UpdatePod(task2, task2Upd)
	cluster.waitAndAssertTaskState(t, "app0001", "task0002", cache.TaskStates().Completed)
	err = cluster.waitForApplicationStateInCore("app0001", partitionName, "Completing")
	assert.NilError(t, err)
	app := cluster.getApplicationFromCore("app0001", partitionName)
	assert.Equal(t, 0, len(app.GetAllRequests()), "asks were not removed from the application")
	assert.Equal(t, 0, len(app.GetAllAllocations()), "allocations were not removed from the application")
}

func TestRejectApplications(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100000000
                vcore: 10
              max:
                memory: 150000000
                vcore: 20
`
	// init and register scheduler
	cluster := MockScheduler{}
	cluster.init()
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err, "update config failed")

	nodeLabels := map[string]string{
		"label1": "key1",
		"label2": "key2",
	}

	// register nodes
	err = cluster.addNode("test.host.01", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err)
	err = cluster.addNode("test.host.02", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err)

	// create app and tasks
	appID := "app0001"
	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 10000000).
		AddResource(siCommon.CPU, 1).
		Build()

	task1 := createTestPod("root.non_exist_queue", appID, "task0001", taskResource)
	cluster.AddPod(task1)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, appID, cache.ApplicationStates().Failed)

	// make the task terminal state
	cluster.DeletePod(task1)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", cache.TaskStates().Completed)
	// make sure the shim side has clean up the failed app
	cluster.waitForApplicationDeletion(t, appID)

	// submit again
	task1 = createTestPod("root.a", appID, "task0001", taskResource)
	cluster.AddPod(task1)

	cluster.waitAndAssertApplicationState(t, appID, cache.ApplicationStates().Running)
	cluster.waitAndAssertTaskState(t, appID, "task0001", cache.TaskStates().Bound)
}

func TestSchedulerRegistrationFailed(t *testing.T) {
	var callback api.ResourceManagerCallback

	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.GetAPIs().SchedulerAPI = test.NewSchedulerAPIMock().RegisterFunction(
		func(request *si.RegisterResourceManagerRequest,
			callback api.ResourceManagerCallback) (response *si.RegisterResourceManagerResponse, e error) {
			return nil, fmt.Errorf("some error")
		})

	ctx := cache.NewContext(mockedAPIProvider)
	shim := newShimSchedulerInternal(ctx, mockedAPIProvider, callback)
	assert.Error(t, shim.Run(), "some error")
	shim.Stop()
}

func TestTaskFailures(t *testing.T) {
	configData := `
partitions:
 -
   name: default
   queues:
     -
       name: root
       submitacl: "*"
       queues:
         -
           name: a
           resources:
             guaranteed:
               memory: 100000000
               vcore: 10
             max:
               memory: 100000000
               vcore: 10
`
	// init and register scheduler
	cluster := MockScheduler{}
	cluster.init()
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	// mock pod bind failures
	cluster.apiProvider.MockBindFn(func(pod *v1.Pod, hostID string) error {
		if pod.Name == "task0001" {
			return fmt.Errorf("mocked error when binding the pod")
		}
		return nil
	})

	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err, "update config failed")

	nodeLabels := map[string]string{
		"label1": "key1",
		"label2": "key2",
	}
	// register nodes
	err = cluster.addNode("test.host.01", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err, "add node failed")
	err = cluster.addNode("test.host.02", nodeLabels, 100000000, 10, 10)
	assert.NilError(t, err, "add node failed")

	// create app and tasks
	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 50000000).
		AddResource(siCommon.CPU, 5).
		Build()
	task1 := createTestPod("root.a", "app0001", "task0001", taskResource)
	task2 := createTestPod("root.a", "app0001", "task0002", taskResource)
	cluster.AddPod(task1)
	cluster.AddPod(task2)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, "app0001", cache.ApplicationStates().Running)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", cache.TaskStates().Failed)
	cluster.waitAndAssertTaskState(t, "app0001", "task0002", cache.TaskStates().Bound)

	// one task get bound, one ask failed, so we are expecting only 1 allocation in the scheduler
	err = cluster.waitAndVerifySchedulerAllocations("root.a",
		"[mycluster]default", "app0001", 1)
	assert.NilError(t, err, "number of allocations is not expected, error")
}

// simulate PVC error during Context.AssumePod() call
func TestAssumePodError(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100000000
                vcore: 10
              max:
                memory: 150000000
                vcore: 20
`
	cluster := MockScheduler{}
	cluster.init()
	binder := test.NewVolumeBinderMock()
	binder.EnableVolumeClaimsError("unable to get volume claims")
	cluster.apiProvider.SetVolumeBinder(binder)
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err, "update config failed")
	addNode(&cluster, "node-1")

	// create app and task which will fail due to simulated volume error
	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 1000).
		AddResource(siCommon.CPU, 1).
		Build()
	pod1 := createTestPod("root.a", "app0001", "task0001", taskResource)
	cluster.AddPod(pod1)

	// expect app to enter Completing state with allocation+ask removed
	err = cluster.waitForApplicationStateInCore("app0001", partitionName, "Completing")
	assert.NilError(t, err)
	app := cluster.getApplicationFromCore("app0001", partitionName)
	assert.Equal(t, 0, len(app.GetAllRequests()), "asks were not removed from the application")
	assert.Equal(t, 0, len(app.GetAllAllocations()), "allocations were not removed from the application")
}

func TestForeignPodTracking(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100000000
                vcore: 10
              max:
                memory: 150000000
                vcore: 20
`
	cluster := MockScheduler{}
	cluster.init()
	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()

	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err, "update config failed")
	addNode(&cluster, "node-1")

	podResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 1000).
		AddResource(siCommon.CPU, 1).
		Build()
	pod1 := createTestPod("root.a", "", "foreign-1", podResource)
	pod1.Spec.SchedulerName = ""
	pod1.Spec.NodeName = "node-1"
	pod2 := createTestPod("root.a", "", "foreign-2", podResource)
	pod2.Spec.SchedulerName = ""
	pod2.Spec.NodeName = "node-1"

	cluster.AddPod(pod1)
	cluster.AddPod(pod2)

	err = cluster.waitAndAssertForeignAllocationInCore(partitionName, "foreign-1", "node-1", true)
	assert.NilError(t, err)
	err = cluster.waitAndAssertForeignAllocationInCore(partitionName, "foreign-2", "node-1", true)
	assert.NilError(t, err)

	// update pod resources
	pod1Copy := pod1.DeepCopy()
	pod1Copy.Spec.Containers[0].Resources.Requests[siCommon.Memory] = *resource.NewQuantity(500, resource.DecimalSI)
	pod1Copy.Spec.Containers[0].Resources.Requests[siCommon.CPU] = *resource.NewMilliQuantity(2000, resource.DecimalSI)

	cluster.UpdatePod(pod1, pod1Copy)
	expectedUsage := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 500).
		AddResource(siCommon.CPU, 2).
		AddResource("pods", 1).
		Build()
	err = cluster.waitAndAssertForeignAllocationResources(partitionName, "foreign-1", "node-1", expectedUsage)
	assert.NilError(t, err)

	// delete pods
	cluster.DeletePod(pod1)
	cluster.DeletePod(pod2)

	err = cluster.waitAndAssertForeignAllocationInCore(partitionName, "foreign-1", "node-1", false)
	assert.NilError(t, err)
	err = cluster.waitAndAssertForeignAllocationInCore(partitionName, "foreign-2", "node-1", false)
	assert.NilError(t, err)
}

func createTestPod(queue string, appID string, taskID string, taskResource *si.Resource) *v1.Pod {
	containers := make([]v1.Container, 0)
	c1Resources := make(map[v1.ResourceName]resource.Quantity)
	for k, v := range taskResource.Resources {
		if k == siCommon.CPU {
			c1Resources[v1.ResourceName(k)] = *resource.NewMilliQuantity(v.Value, resource.DecimalSI)
		} else {
			c1Resources[v1.ResourceName(k)] = *resource.NewQuantity(v.Value, resource.DecimalSI)
		}
	}
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: c1Resources,
		},
	})
	return &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      taskID,
			Namespace: "default",
			UID:       types.UID(taskID),
			Labels: map[string]string{
				constants.LabelApplicationID: appID,
				constants.LabelQueueName:     queue,
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			Containers:    containers,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
}

// Simulate DaemonSet preemption
// Setup:
// * 2 nodes (node-1, node-2)
// * node-1 is almost full w/ 2 pods (pod1Node1, pod2Node2)
// * node-2 is empty
// * submit a DS pod to node-1 (requiredNode)
// * preemption is triggered (pod2Node1)
// * delete pod2Node1
// * test is paused with time.Sleep(), behavior can be observed on the console
func TestDeamonsetPreemption(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: default
`
	cluster := MockScheduler{}
	cluster.init()
	node1 := &v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
		ObjectMeta: apis.ObjectMeta{
			Name: "node-1",
			UID:  types.UID("UUID-node-1"),
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

	node2 := node1.DeepCopy()
	node2.Name = "node-2"
	node2.UID = "UUID-node-2"
	cluster.AddNodeToLister(node1)
	cluster.AddNodeToLister(node2)

	containers := make([]v1.Container, 0)
	c1Resources := make(map[v1.ResourceName]resource.Quantity)
	c1Resources[v1.ResourceCPU] = *resource.NewMilliQuantity(1, resource.DecimalSI)
	c1Resources[v1.ResourceMemory] = *resource.NewScaledQuantity(7, resource.Giga)

	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: c1Resources,
		},
	})

	pod1Node1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "pod1Node1",
			Namespace: "default",
			UID:       types.UID("uid-pod1Node1"),
			Labels: map[string]string{
				constants.LabelApplicationID: "app-1",
				constants.LabelQueueName:     "root.default",
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			Containers:    containers,
			NodeName:      "node-1",
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	pod2Node1 := pod1Node1.DeepCopy()
	pod2Node1.Name = "pod2Node1"
	pod2Node1.Spec.NodeName = "node-1"
	pod2Node1.UID = "uid-pod2Node1"

	cluster.AddPodToLister(pod1Node1)
	cluster.AddPodToLister(pod2Node1)

	assert.NilError(t, cluster.start(), "failed to start cluster")
	defer cluster.stop()
	err := cluster.updateConfig(configData, nil)
	assert.NilError(t, err)
	// wait for YK start
	time.Sleep(time.Second * 2)

	dsPod1 := &v1.Pod{
		TypeMeta: apis.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: apis.ObjectMeta{
			Name:      "daemonset-1",
			Namespace: "default",
			UID:       types.UID("uid-daemonset-1"),
			Labels: map[string]string{
				constants.LabelApplicationID: "app-2",
				constants.LabelQueueName:     "root.default",
			},
			OwnerReferences: []apis.OwnerReference{
				{
					Kind: "DaemonSet",
				},
			},
		},
		Spec: v1.PodSpec{
			SchedulerName: constants.SchedulerName,
			Containers:    containers,
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{
										Key:      "metadata.name",
										Operator: v1.NodeSelectorOpIn, // note: remove this line to cause NodeAffinity predicate to fail (parse error) - no scheduling progress in this case
										Values:   []string{"node-1"},
									},
								},
							},
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
	// submit DaemonSet pod - requiredNode = "node-1"
	cluster.AddPod(dsPod1)

	time.Sleep(time.Second * 5)
	cluster.DeletePod(pod2Node1) // selected by the preemption logic
	time.Sleep(time.Hour)
}
