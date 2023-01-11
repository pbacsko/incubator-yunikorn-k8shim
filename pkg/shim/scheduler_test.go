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
	"bytes"
	"fmt"
	"github.com/apache/yunikorn-k8shim/pkg/appmgmt/general"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"
	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/appmgmt"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/test"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const nodeLabels = "{\"label1\":\"key1\",\"label2\":\"key2\"}"

func TestMemUsage(t *testing.T) {
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
                {memory: 64G, vcore: 100}
              max:
                {memory: 64G, vcore: 100}
`
	file, err := os.OpenFile("/tmp/memstats.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	assert.NilError(t, err)

	// init and register scheduler
	events.SetRecorder(events.NewMockedRecorder())
	cluster := MockScheduler{}
	cluster.init()
	cluster.start()
	defer cluster.stop()

	// ensure scheduler running
	cluster.waitForSchedulerState(t, SchedulerStates().Running)
	err = cluster.updateConfig(configData)
	assert.NilError(t, err, "update config failed")

	cnt := 0
	logMemStats(file, cnt, "BEFORE_ADD_NODES")

	// register nodes
	for i := 0; i < 100; i++ {
		addNode(cluster, "test.host."+strconv.Itoa(i))
	}
	time.Sleep(10 * time.Second)
	for ; cnt < 50; cnt++ {
		logMemStats(file, cnt, "BEFORE_ADD_PODS")

		// Add pods
		pods := getTestPods(100, 10, "root.a")
		for _, pod := range pods {
			addPod(cluster, pod)
		}
		time.Sleep(10 * time.Second)

		// Update pods to Running state
		newPods := make([]*v1.Pod, 0)
		for _, pod := range pods {
			oldPod, newPod := updatePodWithRunningStateAndNode(t, cluster, pod, v1.PodRunning)
			updatePod(cluster, oldPod, newPod)
			newPods = append(newPods, newPod)
		}
		pods = newPods
		time.Sleep(10 * time.Second)
		logMemStats(file, cnt, "AFTER_ADD_PODS")

		// Finish pods
		newPods = make([]*v1.Pod, 0)
		for _, pod := range pods {
			oldPod, newPod := updatePodWithRunningStateAndNode(t, cluster, pod, v1.PodSucceeded)
			updatePod(cluster, oldPod, newPod)
			newPods = append(newPods, newPod)
		}
		pods = newPods
		time.Sleep(10 * time.Second)
		logMemStats(file, cnt, "AFTER_FINISH_PODS")
	}
}

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
	cluster.start()
	defer cluster.stop()

	// ensure scheduler running
	cluster.waitForSchedulerState(t, SchedulerStates().Running)
	err := cluster.updateConfig(configData)
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
	cluster.addApplication("app0001", "root.a")
	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 10000000).
		AddResource(siCommon.CPU, 1).
		Build()
	cluster.addTask("app0001", "task0001", taskResource)
	cluster.addTask("app0001", "task0002", taskResource)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, "app0001", cache.ApplicationStates().Running)
	cluster.waitAndAssertTaskState(t, "app0001", "task0001", cache.TaskStates().Bound)
	cluster.waitAndAssertTaskState(t, "app0001", "task0002", cache.TaskStates().Bound)
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
	cluster.start()
	defer cluster.stop()

	// ensure scheduler state
	cluster.waitForSchedulerState(t, SchedulerStates().Running)
	err := cluster.updateConfig(configData)
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

	// add app to context
	appID := "app0001"
	cluster.addApplication(appID, "root.non_exist_queue")

	// create app and tasks
	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 10000000).
		AddResource(siCommon.CPU, 1).
		Build()
	cluster.addTask(appID, "task0001", taskResource)

	// wait for scheduling app and tasks
	// verify app state
	cluster.waitAndAssertApplicationState(t, appID, cache.ApplicationStates().Failed)

	// remove the application
	// remove task first or removeApplication will fail
	cluster.context.RemoveTask(appID, "task0001")
	err = cluster.removeApplication(appID)
	assert.Assert(t, err == nil)

	// submit the app again
	cluster.addApplication(appID, "root.a")
	cluster.addTask(appID, "task0001", taskResource)
	cluster.waitAndAssertApplicationState(t, appID, cache.ApplicationStates().Running)
	cluster.waitAndAssertTaskState(t, appID, "task0001", cache.TaskStates().Bound)
}

func TestSchedulerRegistrationFailed(t *testing.T) {
	var callback api.ResourceManagerCallback

	mockedAMProtocol := cache.NewMockedAMProtocol()
	mockedAPIProvider := client.NewMockedAPIProvider(false)
	mockedAPIProvider.GetAPIs().SchedulerAPI = test.NewSchedulerAPIMock().RegisterFunction(
		func(request *si.RegisterResourceManagerRequest,
			callback api.ResourceManagerCallback) (response *si.RegisterResourceManagerResponse, e error) {
			return nil, fmt.Errorf("some error")
		})

	ctx := cache.NewContext(mockedAPIProvider)
	shim := newShimSchedulerInternal(ctx, mockedAPIProvider,
		appmgmt.NewAMService(mockedAMProtocol, mockedAPIProvider), callback)
	shim.Run()
	defer shim.Stop()

	err := waitShimSchedulerState(shim, SchedulerStates().Stopped, 5*time.Second)
	assert.NilError(t, err)
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
	cluster.start()
	defer cluster.stop()

	// mock pod bind failures
	cluster.apiProvider.MockBindFn(func(pod *v1.Pod, hostID string) error {
		if pod.Name == "task0001" {
			return fmt.Errorf("mocked error when binding the pod")
		}
		return nil
	})

	// ensure scheduler state
	cluster.waitForSchedulerState(t, SchedulerStates().Running)
	err := cluster.updateConfig(configData)
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
	cluster.addApplication("app0001", "root.a")
	taskResource := common.NewResourceBuilder().
		AddResource(siCommon.Memory, 50000000).
		AddResource(siCommon.CPU, 5).
		Build()
	cluster.addTask("app0001", "task0001", taskResource)
	cluster.addTask("app0001", "task0002", taskResource)
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

func addNode(cluster MockScheduler, name string) {
	node := &v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: false,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			UID:  types.UID("UUID-" + name),
		},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{
				{Type: v1.NodeReady, Status: v1.ConditionTrue},
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(16000, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewScaledQuantity(32, resource.Giga),
			},
		},
	}

	cluster.context.AddNode(node)
}

func addPod(cluster MockScheduler, pod *v1.Pod) {
	cluster.context.AddPodToCache(pod)
	mgr := cluster.scheduler.appManager.GetManagerByName("general")
	if generalMgr, ok := mgr.(*general.Manager); ok {
		generalMgr.AddPod(pod)
	}
}

func updatePod(cluster MockScheduler, oldPod, newPod *v1.Pod) {
	cluster.context.UpdatePodInCache(oldPod, newPod)
	mgr := cluster.scheduler.appManager.GetManagerByName("general")
	if generalMgr, ok := mgr.(*general.Manager); ok {
		generalMgr.UpdatePod(oldPod, newPod)
	}
}

func getTestPods(noApps, noTasksPerApp int, queue string) []*v1.Pod {
	pods := make([]*v1.Pod, 0)
	resources := make(map[v1.ResourceName]resource.Quantity)
	resources[v1.ResourceCPU] = *resource.NewMilliQuantity(10, resource.DecimalSI)
	resources[v1.ResourceMemory] = *resource.NewScaledQuantity(10, resource.Mega)

	for i := 0; i < noApps; i++ {
		appId := "app000" + strconv.Itoa(i) + "-" + strconv.FormatInt(time.Now().UnixMilli(), 10)
		for j := 0; j < noTasksPerApp; j++ {
			taskName := "task000" + strconv.Itoa(j)
			pod := &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: taskName,
					UID:  types.UID("UID-" + appId + "-" + taskName),
					Annotations: map[string]string{
						constants.AnnotationApplicationID: appId,
						constants.AnnotationQueueName:     queue,
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "container-01",
							Resources: v1.ResourceRequirements{
								Requests: resources,
							},
						},
					},
				},
			}
			pods = append(pods, pod)
		}
	}

	return pods
}

func updatePodWithRunningStateAndNode(t *testing.T, cluster MockScheduler, pod *v1.Pod, phase v1.PodPhase) (*v1.Pod, *v1.Pod) {
	old := pod.DeepCopy()
	pod.Status.Phase = phase
	node, ok := cluster.context.GetAssignedNodeForPod(string(pod.GetUID()))
	if !ok {
		cluster.context.DumpCache()
		t.Fatal("Node not found in cache")
	}
	pod.Spec.NodeName = node

	return old, pod
}

func waitShimSchedulerState(shim *KubernetesShim, expectedState string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if shim.GetSchedulerState() == expectedState {
			log.Logger().Info("waiting for state",
				zap.String("expect", expectedState),
				zap.String("current", shim.GetSchedulerState()))
			return nil
		}
		time.Sleep(1 * time.Second)
		if time.Now().After(deadline) {
			return fmt.Errorf("scheduler has not reached expected state %s in %d seconds, current state: %s",
				expectedState, deadline.Second(), shim.GetSchedulerState())
		}
	}
}

func logMemStats(file *os.File, i int, phase string) {
	memStats := runtime.MemStats{}
	runtime.ReadMemStats(&memStats)
	fmt.Printf("MemStats after iteration #%d, phase %s\n", i, phase)
	fmt.Println("Alloc", memStats.Alloc)
	fmt.Println("TotalAlloc", memStats.TotalAlloc)
	fmt.Println("Sys", memStats.Sys)
	fmt.Println("HeapAlloc", memStats.HeapAlloc)
	fmt.Println("HeapSys", memStats.HeapSys)
	fmt.Println("HeapIdle", memStats.HeapIdle)
	fmt.Println("HeapInuse", memStats.HeapInuse)
	fmt.Println("HeapReleased", memStats.HeapReleased)
	fmt.Println("HeapObjects", memStats.HeapObjects)

	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("MemStats after iteration #%d, phase %s\n", i, phase))
	buf.WriteString(fmt.Sprintln("Alloc", memStats.Alloc))
	buf.WriteString(fmt.Sprintln("TotalAlloc", memStats.TotalAlloc))
	buf.WriteString(fmt.Sprintln("Sys", memStats.Sys))
	buf.WriteString(fmt.Sprintln("HeapAlloc", memStats.HeapAlloc))
	buf.WriteString(fmt.Sprintln("HeapSys", memStats.HeapSys))
	buf.WriteString(fmt.Sprintln("HeapIdle", memStats.HeapIdle))
	buf.WriteString(fmt.Sprintln("HeapInuse", memStats.HeapInuse))
	buf.WriteString(fmt.Sprintln("HeapReleased", memStats.HeapReleased))
	buf.WriteString(fmt.Sprintln("HeapObjects", memStats.HeapObjects))
	buf.WriteString("****")
	buf.WriteTo(file)
}
