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
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
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
const numNodes = 2000

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
                {memory: 1500G, vcore: 100}
`
	file, err := os.OpenFile("/tmp/memstats.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	assert.NilError(t, err)
	log.GetZapConfigs().Level.SetLevel(zapcore.InfoLevel)

	// init and register scheduler
	events.SetRecorder(events.NewMockedRecorder())
	cluster := MockScheduler{}
	cluster.init()
	cluster.start()
	defer cluster.stop()

	f, err := os.Create("/tmp/TestMemUsage-cpu.prof")
	if err != nil {
		fmt.Println(err)
		return
	}

	h, err := os.Create("/tmp/TestMemUsage-heap.prof")
	if err != nil {
		fmt.Println(err)
		return
	}

	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	defer pprof.WriteHeapProfile(h)

	// ensure scheduler running
	cluster.waitForSchedulerState(t, SchedulerStates().Running)
	err = cluster.updateConfig(configData)
	assert.NilError(t, err, "update config failed")

	cnt := 0
	logMemStats(file, cnt, "BEFORE_ADD_NODES")

	// register nodes
	for i := 0; i < numNodes; i++ {
		addNode(cluster, "test.host."+strconv.Itoa(i))
	}
	time.Sleep(5 * time.Second)

	for ; cnt < 10; cnt++ {
		logMemStats(file, cnt, "BEFORE_ADD_PODS")

		// Add pods
		pods := getTestPods(10, 100, "root.a")
		for _, pod := range pods {
			addPod(cluster, pod)
		}
		time.Sleep(5 * time.Second)

		// Update pods to Running state
		for _, pod := range pods {
			updatePodWithRunningStateAndNode(cluster, pod, v1.PodRunning)
		}
		time.Sleep(5 * time.Second)
		logMemStats(file, cnt, "AFTER_ADD_PODS")

		// Finish pods
		for _, pod := range pods {
			updatePodWithRunningState(cluster, pod, v1.PodSucceeded)
		}
		time.Sleep(20 * time.Second)
		logMemStats(file, cnt, "AFTER_FINISH_PODS")
	}
}

func TestMemUsage_PendingPods(t *testing.T) {
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
                {memory: 150G, vcore: 100}
              max:
                {memory: 300G, vcore: 100}
`
	file, err := os.OpenFile("/tmp/memstats.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	assert.NilError(t, err)
	log.GetZapConfigs().Level.SetLevel(zapcore.InfoLevel)

	// init and register scheduler
	events.SetRecorder(events.NewMockedRecorder())
	cluster := MockScheduler{}
	cluster.init()
	cluster.start()
	defer cluster.stop()

	f, err := os.Create("/tmp/TestMemUsage_PendingPods-cpu.prof")
	if err != nil {
		fmt.Println(err)
		return
	}

	h, err := os.Create("/tmp/TestMemUsage_PendingPods-heap.prof")
	if err != nil {
		fmt.Println(err)
		return
	}

	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	defer pprof.WriteHeapProfile(h)

	// ensure scheduler running
	cluster.waitForSchedulerState(t, SchedulerStates().Running)
	err = cluster.updateConfig(configData)
	assert.NilError(t, err, "update config failed")

	cnt := 0
	logMemStats(file, cnt, "BEFORE_ADD_NODES")

	// register nodes
	for i := 0; i < numNodes; i++ {
		addNode(cluster, "test.host."+strconv.Itoa(i))
	}
	time.Sleep(5 * time.Second)

	for ; cnt < 3; cnt++ {
		// Add pods
		pods := getTestPods(5, 2000, "root.a")
		for _, pod := range pods {
			addPod(cluster, pod)
		}
		time.Sleep(5 * time.Second)

		// Update pods to Running state as they get scheduled
		runnablePods := make(map[string]struct{})
		finishedPods := make(map[string]struct{})
		mapLock := &sync.Mutex{}

		var wg sync.WaitGroup
		wg.Add(2)

		go updatePodsWithRunningStateAndNodeForAssignedOnly(cluster,
			pods, v1.PodRunning, mapLock, runnablePods, finishedPods, &wg)
		go finishRunningPodsInBatches(cluster,
			pods, mapLock, runnablePods, finishedPods, &wg)

		fmt.Println("Testcase: waiting")
		wg.Wait()
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
				"pods":            *resource.NewScaledQuantity(110, resource.Scale(0)),
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
	resources[v1.ResourceMemory] = *resource.NewScaledQuantity(1, resource.Giga)

	for i := 0; i < noApps; i++ {
		appId := "app000" + strconv.Itoa(i) + "-" + strconv.FormatInt(time.Now().UnixMilli(), 10)
		for j := 0; j < noTasksPerApp; j++ {
			taskName := appId + "-" + "task000" + strconv.Itoa(j)
			pod := &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: taskName,
					UID:  types.UID("UID-" + taskName),
					Annotations: map[string]string{
						constants.AnnotationApplicationID: appId,
						constants.AnnotationQueueName:     queue,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
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

func updatePodWithRunningState(cluster MockScheduler, pod *v1.Pod, phase v1.PodPhase) {
	old := pod.DeepCopy()
	pod.Status.Phase = phase
	updatePod(cluster, old, pod)
}

func updatePodWithRunningStateAndNode(cluster MockScheduler, pod *v1.Pod, phase v1.PodPhase) {
	old := pod.DeepCopy()
	pod.Status.Phase = phase
	node, ok := cluster.context.GetAssignedNodeForPod(string(pod.GetUID()))
	if !ok {
		log.Logger().Info("Node not found for pod, likely it hasn't been scheduled",
			zap.String("pod", pod.Name))
		return
	}
	pod.Spec.NodeName = node
	updatePod(cluster, old, pod)
}

func finishRunningPodsInBatches(cluster MockScheduler,
	pods []*v1.Pod,
	mapLock *sync.Mutex,
	runnable map[string]struct{},
	finished map[string]struct{},
	wg *sync.WaitGroup) {
	noRemainingCount := 0
	for {
		// total assigned (bound) pods
		remaining := make(map[string]struct{}, 0)
		for pod, _ := range cluster.context.GetAssignedPods() {
			remaining[pod] = struct{}{}
		}

		if len(remaining) == 0 {
			noRemainingCount++
			if noRemainingCount == 5 {
				break
			}
			fmt.Println("Running -> Succeeded: No assigned pods, waiting...")
			time.Sleep(time.Second * 2)
			continue
		}

		for {
			if len(remaining) == 0 {
				break
			}

			for _, pod := range pods {
				key := string(pod.GetUID())

				mapLock.Lock()
				_, podRunnable := runnable[key]
				mapLock.Unlock()

				if !podRunnable {
					continue
				}

				if _, ok := remaining[key]; ok {
					old := pod.DeepCopy()
					pod.Status.Phase = v1.PodSucceeded
					updatePod(cluster, old, pod)
					delete(remaining, key)
					mapLock.Lock()
					finished[key] = struct{}{}
					mapLock.Unlock()
				}
			}
			time.Sleep(time.Second)
		}
		time.Sleep(time.Second)
	}

	fmt.Println("Running -> Succeeded: finish")
	wg.Done()
}

func updatePodsWithRunningStateAndNodeForAssignedOnly(cluster MockScheduler,
	pods []*v1.Pod,
	phase v1.PodPhase,
	mapLock *sync.Mutex,
	runnable map[string]struct{},
	finished map[string]struct{},
	wg *sync.WaitGroup) {
	noAssignedCount := 0
	for {
		assigned := cluster.context.GetAssignedPods()
		fmt.Printf("->Running: %d assigned pods\n", len(assigned))
		if len(assigned) == 0 {
			noAssignedCount++
			if noAssignedCount == 5 {
				break
			}
			fmt.Println("Scheduled->Running: No assigned pods, waiting...")
			time.Sleep(time.Second * 2)
			continue
		}
		for _, pod := range pods {
			key := string(pod.GetUID())
			if node, ok := assigned[key]; ok {
				mapLock.Lock()
				_, podFinished := finished[key]
				mapLock.Unlock()
				if podFinished {
					continue
				}

				old := pod.DeepCopy()
				pod.Status.Phase = phase
				pod.Spec.NodeName = node
				updatePod(cluster, old, pod)
				mapLock.Lock()
				runnable[key] = struct{}{}
				mapLock.Unlock()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	wg.Done()
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
	runtime.GC()
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
	fmt.Println("NumGC", memStats.NumGC)
	fmt.Println("GCCPUFraction", memStats.GCCPUFraction)

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
	buf.WriteString(fmt.Sprintln("NumGC", memStats.NumGC))
	buf.WriteString(fmt.Sprintln("GCCPUFraction", memStats.GCCPUFraction))
	buf.WriteString("****")
	buf.WriteTo(file)
}
