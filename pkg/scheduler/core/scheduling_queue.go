/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This file contains structures that implement scheduling queue types.
// Scheduling queues hold pods waiting to be scheduled. This file has two types
// of scheduling queue: 1) a FIFO, which is mostly the same as cache.FIFO, 2) a
// priority queue which has two sub queues. One sub-queue holds pods that are
// being considered for scheduling. This is called activeQ. Another queue holds
// pods that are already tried and are determined to be unschedulable. The latter
// is called unschedulableQ.
// FIFO is here for flag-gating purposes and allows us to use the traditional
// scheduling queue when util.PodPriorityEnabled() returns false.

package core

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/golang/glog"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	priorityutil "k8s.io/kubernetes/pkg/scheduler/algorithm/priorities/util"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// SchedulingQueue is an interface for a queue to store pods waiting to be scheduled.
// The interface follows a pattern similar to cache.FIFO and cache.Heap and
// makes it easy to use those data structures as a SchedulingQueue.
type SchedulingQueue interface {
	Run()
	Add(pod *v1.Pod) error
	AddIfNotPresent(pod *v1.Pod) error
	AddUnschedulableIfNotPresent(pod *v1.Pod) error
	Pop() (*v1.Pod, error)
	Update(oldPod, newPod *v1.Pod) error
	Delete(pod *v1.Pod) error
	MoveAllToActiveQueue()
	AssignedPodAdded(pod *v1.Pod)
	AssignedPodUpdated(pod *v1.Pod)
	WaitingPodsForNode(nodeName string) []*v1.Pod
	WaitingPods() []*v1.Pod
}

// NewSchedulingQueue initializes a new scheduling queue. If pod priority is
// enabled a priority queue is returned. If it is disabled, a FIFO is returned.
func NewSchedulingQueue(stop <-chan struct{}) SchedulingQueue {
	if util.PodPriorityEnabled() {
		return NewPriorityQueue(stop)
	}
	return NewFIFO()
}

// FIFO is basically a simple wrapper around cache.FIFO to make it compatible
// with the SchedulingQueue interface.
type FIFO struct {
	*cache.FIFO
}

var _ = SchedulingQueue(&FIFO{}) // Making sure that FIFO implements SchedulingQueue.

// Run is a noop here but needed to implement SchedulingQueue
func (f *FIFO) Run() {
}

// Add adds a pod to the FIFO.
func (f *FIFO) Add(pod *v1.Pod) error {
	return f.FIFO.Add(pod)
}

// AddIfNotPresent adds a pod to the FIFO if it is absent in the FIFO.
func (f *FIFO) AddIfNotPresent(pod *v1.Pod) error {
	return f.FIFO.AddIfNotPresent(pod)
}

// AddUnschedulableIfNotPresent adds an unschedulable pod back to the queue. In
// FIFO it is added to the end of the queue.
func (f *FIFO) AddUnschedulableIfNotPresent(pod *v1.Pod) error {
	return f.FIFO.AddIfNotPresent(pod)
}

// Update updates a pod in the FIFO.
func (f *FIFO) Update(oldPod, newPod *v1.Pod) error {
	return f.FIFO.Update(newPod)
}

// Delete deletes a pod in the FIFO.
func (f *FIFO) Delete(pod *v1.Pod) error {
	return f.FIFO.Delete(pod)
}

// Pop removes the head of FIFO and returns it.
// This is just a copy/paste of cache.Pop(queue Queue) from fifo.go that scheduler
// has always been using. There is a comment in that file saying that this method
// shouldn't be used in production code, but scheduler has always been using it.
// This function does minimal error checking.
func (f *FIFO) Pop() (*v1.Pod, error) {
	var result interface{}
	f.FIFO.Pop(func(obj interface{}) error {
		result = obj
		return nil
	})
	return result.(*v1.Pod), nil
}

// WaitingPods returns all the waiting pods in the queue.
func (f *FIFO) WaitingPods() []*v1.Pod {
	result := []*v1.Pod{}
	for _, pod := range f.FIFO.List() {
		result = append(result, pod.(*v1.Pod))
	}
	return result
}

// FIFO does not need to react to events, as all pods are always in the active
// scheduling queue anyway.

// AssignedPodAdded does nothing here.
func (f *FIFO) AssignedPodAdded(pod *v1.Pod) {}

// AssignedPodUpdated does nothing here.
func (f *FIFO) AssignedPodUpdated(pod *v1.Pod) {}

// MoveAllToActiveQueue does nothing in FIFO as all pods are always in the active queue.
func (f *FIFO) MoveAllToActiveQueue() {}

// WaitingPodsForNode returns pods that are nominated to run on the given node,
// but FIFO does not support it.
func (f *FIFO) WaitingPodsForNode(nodeName string) []*v1.Pod {
	return nil
}

// NewFIFO creates a FIFO object.
func NewFIFO() *FIFO {
	return &FIFO{FIFO: cache.NewFIFO(cache.MetaNamespaceKeyFunc)}
}

// NominatedNodeName returns nominated node name of a Pod.
func NominatedNodeName(pod *v1.Pod) string {
	return pod.Status.NominatedNodeName
}

// PriorityQueue implements a scheduling queue. It is an alternative to FIFO.
// The head of PriorityQueue is the highest priority pending pod. This structure
// has two sub queues. One sub-queue holds pods that are being considered for
// scheduling. This is called activeQ and is a Heap. Another queue holds
// pods that are already tried and are determined to be unschedulable. The latter
// is called unschedulableQ.
type PriorityQueue struct {
	lock  sync.RWMutex
	cond  sync.Cond
	stop  <-chan struct{}
	clock util.Clock

	// activeQ is heap structure that scheduler actively looks at to find pods to
	// schedule. Head of heap is the highest priority pod.
	activeQ *util.Heap
	// unschedulableQ holds pods that have been tried and determined unschedulable.
	unschedulableQ *UnschedulablePodsMap
	// nominatedPods is a map keyed by a node name and the value is a list of
	// pods which are nominated to run on the node. These are pods which can be in
	// the activeQ or unschedulableQ.
	nominatedPods map[string][]*v1.Pod
	// podBackoff tracks pods which are awaiting a backoff period before being added to activeQ
	podBackoff *util.PodBackoff
	// receivedMoveRequest is set to true whenever we receive a request to move a
	// pod from the unschedulableQ to the activeQ, and is set to false, when we pop
	// a pod from the activeQ. It indicates if we received a move request when a
	// pod was in flight (we were trying to schedule it). In such a case, we put
	// the pod back into the activeQ if it is determined unschedulable.
	receivedMoveRequest bool
}

// Making sure that PriorityQueue implements SchedulingQueue.
var _ = SchedulingQueue(&PriorityQueue{})

// NewPriorityQueue creates a PriorityQueue object.
func NewPriorityQueue(stopEverything <-chan struct{}) *PriorityQueue {
	return NewPriorityQueueWithClock(stopEverything, util.RealClock{})
}

// NewPriorityQueueWithClock creates a PriorityQueue object using the passed clock.
func NewPriorityQueueWithClock(stopEverything <-chan struct{}, clock util.Clock) *PriorityQueue {
	pq := &PriorityQueue{
		stop:           stopEverything,
		clock:          clock,
		activeQ:        util.NewHeap(cache.MetaNamespaceKeyFunc, util.HigherPriorityPod),
		unschedulableQ: newUnschedulablePodsMap(),
		nominatedPods:  map[string][]*v1.Pod{},
		podBackoff:     util.CreatePodBackoffWithClock(1*time.Second, 10*time.Second, clock),
	}
	pq.cond.L = &pq.lock
	return pq
}

// addNominatedPodIfNeeded adds a pod to nominatedPods if it has a NominatedNodeName and it does not
// already exist in the map. Adding an existing pod is not going to update the pod.
func (p *PriorityQueue) addNominatedPodIfNeeded(pod *v1.Pod) {
	nnn := NominatedNodeName(pod)
	if len(nnn) > 0 {
		for _, np := range p.nominatedPods[nnn] {
			if np.UID == pod.UID {
				glog.Errorf("Pod %v/%v already exists in the nominated map!", pod.Namespace, pod.Name)
				return
			}
		}
		p.nominatedPods[nnn] = append(p.nominatedPods[nnn], pod)
	}
}

// deleteNominatedPodIfExists deletes a pod from the nominatedPods.
func (p *PriorityQueue) deleteNominatedPodIfExists(pod *v1.Pod) {
	nnn := NominatedNodeName(pod)
	if len(nnn) > 0 {
		for i, np := range p.nominatedPods[nnn] {
			if np.UID == pod.UID {
				p.nominatedPods[nnn] = append(p.nominatedPods[nnn][:i], p.nominatedPods[nnn][i+1:]...)
				if len(p.nominatedPods[nnn]) == 0 {
					delete(p.nominatedPods, nnn)
				}
				break
			}
		}
	}
}

// updateNominatedPod updates a pod in the nominatedPods.
func (p *PriorityQueue) updateNominatedPod(oldPod, newPod *v1.Pod) {
	// Even if the nominated node name of the Pod is not changed, we must delete and add it again
	// to ensure that its pointer is updated.
	p.deleteNominatedPodIfExists(oldPod)
	p.addNominatedPodIfNeeded(newPod)
}

// Add adds a pod to the active queue. It should be called only when a new pod
// is added so there is no chance the pod is already in either queue.
func (p *PriorityQueue) Add(pod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	err := p.activeQ.Add(pod)
	if err != nil {
		glog.Errorf("Error adding pod %v to the scheduling queue: %v", pod.Name, err)
	} else {
		if p.unschedulableQ.get(pod) != nil {
			glog.Errorf("Error: pod %v is already in the unschedulable queue.", pod.Name)
			p.deleteNominatedPodIfExists(pod)
			p.unschedulableQ.delete(pod)
		}
		p.addNominatedPodIfNeeded(pod)
		p.cond.Broadcast()
	}
	return err
}

// AddIfNotPresent adds a pod to the active queue if it is not present in any of
// the two queues. If it is present in any, it doesn't do any thing.
func (p *PriorityQueue) AddIfNotPresent(pod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.unschedulableQ.get(pod) != nil {
		return nil
	}
	if _, exists, _ := p.activeQ.Get(pod); exists {
		return nil
	}
	err := p.activeQ.Add(pod)
	if err != nil {
		glog.Errorf("Error adding pod %v to the scheduling queue: %v", pod.Name, err)
	} else {
		p.addNominatedPodIfNeeded(pod)
		p.cond.Broadcast()
	}
	return err
}

func isPodUnschedulable(pod *v1.Pod) bool {
	_, cond := podutil.GetPodCondition(&pod.Status, v1.PodScheduled)
	return cond != nil && cond.Status == v1.ConditionFalse && cond.Reason == v1.PodReasonUnschedulable
}

// AddUnschedulableIfNotPresent does nothing if the pod is present in either
// queue. Otherwise it adds the pod to the unschedulable queue if
// p.receivedMoveRequest is false, and to the activeQ if p.receivedMoveRequest is true.
func (p *PriorityQueue) AddUnschedulableIfNotPresent(pod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.unschedulableQ.get(pod) != nil {
		return fmt.Errorf("pod is already present in unschedulableQ")
	}
	if _, exists, _ := p.activeQ.Get(pod); exists {
		return fmt.Errorf("pod is already present in the activeQ")
	}
	if !p.receivedMoveRequest && isPodUnschedulable(pod) {
		p.unschedulableQ.addOrUpdate(pod)
		p.addNominatedPodIfNeeded(pod)
		return nil
	}
	err := p.activeQ.Add(pod)
	if err == nil {
		p.addNominatedPodIfNeeded(pod)
		p.cond.Broadcast()
	}
	return err
}

// Pop removes the head of the active queue and returns it. It blocks if the
// activeQ is empty and waits until a new item is added to the queue. It also
// clears receivedMoveRequest to mark the beginning of a new scheduling cycle.
func (p *PriorityQueue) Pop() (*v1.Pod, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for p.activeQ.Len() == 0 {
		p.cond.Wait()
	}
	obj, err := p.activeQ.Pop()
	if err != nil {
		return nil, err
	}
	pod := obj.(*v1.Pod)
	p.deleteNominatedPodIfExists(pod)
	p.receivedMoveRequest = false
	return pod, err
}

// isPodUpdated checks if the pod is updated in a way that it may have become
// schedulable. It drops status of the pod and compares it with old version.
func isPodUpdated(oldPod, newPod *v1.Pod) bool {
	strip := func(pod *v1.Pod) *v1.Pod {
		p := pod.DeepCopy()
		p.ResourceVersion = ""
		p.Generation = 0
		p.Status = v1.PodStatus{}
		return p
	}
	return !reflect.DeepEqual(strip(oldPod), strip(newPod))
}

// Update updates a pod in the active queue if present. Otherwise, it removes
// the item from the unschedulable queue and adds the updated one to the active
// queue.
func (p *PriorityQueue) Update(oldPod, newPod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	// If the pod is already in the active queue, just update it there.
	if _, exists, _ := p.activeQ.Get(newPod); exists {
		p.updateNominatedPod(oldPod, newPod)
		err := p.activeQ.Update(newPod)
		return err
	}
	// If the pod is in the unschedulable queue, updating it may make it schedulable.
	if usPod := p.unschedulableQ.get(newPod); usPod != nil {
		p.updateNominatedPod(oldPod, newPod)
		if isPodUpdated(oldPod, newPod) {
			p.unschedulableQ.delete(usPod)
			oldPodID := types.NamespacedName{
				Namespace: oldPod.Namespace,
				Name:      oldPod.Name,
			}
			_ = p.podBackoff.CancelPodBackoff(oldPodID)
			err := p.activeQ.Add(newPod)
			if err == nil {
				p.cond.Broadcast()
			}
			return err
		}
		p.unschedulableQ.addOrUpdate(newPod)
		return nil
	}
	// If pod is not in any of the two queue, we put it in the active queue.
	err := p.activeQ.Add(newPod)
	if err == nil {
		p.addNominatedPodIfNeeded(newPod)
		p.cond.Broadcast()
	}
	return err
}

// Delete deletes the item from either of the two queues. It assumes the pod is
// only in one queue.
func (p *PriorityQueue) Delete(pod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.deleteNominatedPodIfExists(pod)
	err := p.activeQ.Delete(pod)
	if err != nil { // The item was probably not found in the activeQ.
		p.unschedulableQ.delete(pod)
		podID := types.NamespacedName{
			Namespace: pod.Namespace,
			Name:      pod.Name,
		}
		_ = p.podBackoff.CancelPodBackoff(podID)
	}
	return nil
}

// AssignedPodAdded is called when a bound pod is added. Creation of this pod
// may make pending pods with matching affinity terms schedulable.
func (p *PriorityQueue) AssignedPodAdded(pod *v1.Pod) {
	p.movePodsToActiveQueue(p.getUnschedulablePodsWithMatchingAffinityTerm(pod))
}

// AssignedPodUpdated is called when a bound pod is updated. Change of labels
// may make pending pods with matching affinity terms schedulable.
func (p *PriorityQueue) AssignedPodUpdated(pod *v1.Pod) {
	p.movePodsToActiveQueue(p.getUnschedulablePodsWithMatchingAffinityTerm(pod))
}

// MoveAllToActiveQueue moves all pods from unschedulableQ to activeQ. This
// function adds all pods and then signals the condition variable to ensure that
// if Pop() is waiting for an item, it receives it after all the pods are in the
// queue and the head is the highest priority pod.
func (p *PriorityQueue) MoveAllToActiveQueue() {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, pod := range p.unschedulableQ.pods {
		p.ScheduleWithBackoff(pod)
	}
	p.receivedMoveRequest = true
}

func (p *PriorityQueue) movePodsToActiveQueue(pods []*v1.Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, pod := range pods {
		p.ScheduleWithBackoff(pod)
	}
	p.receivedMoveRequest = true
}

// getUnschedulablePodsWithMatchingAffinityTerm returns unschedulable pods which have
// any affinity term that matches "pod".
func (p *PriorityQueue) getUnschedulablePodsWithMatchingAffinityTerm(pod *v1.Pod) []*v1.Pod {
	p.lock.RLock()
	defer p.lock.RUnlock()
	var podsToMove []*v1.Pod
	for _, up := range p.unschedulableQ.pods {
		affinity := up.Spec.Affinity
		if affinity != nil && affinity.PodAffinity != nil {
			terms := predicates.GetPodAffinityTerms(affinity.PodAffinity)
			for _, term := range terms {
				namespaces := priorityutil.GetNamespacesFromPodAffinityTerm(up, &term)
				selector, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
				if err != nil {
					glog.Errorf("Error getting label selectors for pod: %v.", up.Name)
				}
				if priorityutil.PodMatchesTermsNamespaceAndSelector(pod, namespaces, selector) {
					podsToMove = append(podsToMove, up)
					break
				}
			}
		}
	}
	return podsToMove
}

// WaitingPodsForNode returns pods that are nominated to run on the given node,
// but they are waiting for other pods to be removed from the node before they
// can be actually scheduled.
func (p *PriorityQueue) WaitingPodsForNode(nodeName string) []*v1.Pod {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if list, ok := p.nominatedPods[nodeName]; ok {
		return list
	}
	return nil
}

// WaitingPods returns all the waiting pods in the queue.
func (p *PriorityQueue) WaitingPods() []*v1.Pod {
	p.lock.Lock()
	defer p.lock.Unlock()

	result := []*v1.Pod{}
	for _, pod := range p.activeQ.List() {
		result = append(result, pod.(*v1.Pod))
	}
	for _, pod := range p.unschedulableQ.pods {
		result = append(result, pod)
	}
	return result
}

// Run starts the goroutine to pump from podBackoff to activeQ
func (p *PriorityQueue) Run() {
	go wait.Until(p.moveNextBackoffToActive, 0, p.stop)
}

// If possible moves a pod from podBackoff to active and returns,
// otherwise blocks until a state change or the stop channel is closed
func (p *PriorityQueue) moveNextBackoffToActive() {
	found, nextPodID := p.podBackoff.PopCompletedBackoff(p.stop)
	if !found {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	// unschedulableQ uses a different ID scheme
	podString := nextPodID.Name + "_" + nextPodID.Namespace
	pod := p.unschedulableQ.pods[podString]
	if pod == nil {
		glog.Errorf("Found backoff completed pod (%s) but pod is missing from unschedulableQ", podString)
	} else {
		p.activeQ.Add(pod)
		p.unschedulableQ.delete(pod)
		p.cond.Broadcast()
	}
}

// ScheduleWithBackoff starts tracking backoff for a pod to be added to activeQ upon
// backoff completion
func (p *PriorityQueue) ScheduleWithBackoff(pod *v1.Pod) {
	podID := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      pod.Name,
	}
	if !p.podBackoff.IsPodBackingOff(podID) {
		p.podBackoff.BackoffPod(podID)
	}
}

// UnschedulablePodsMap holds pods that cannot be scheduled. This data structure
// is used to implement unschedulableQ.
type UnschedulablePodsMap struct {
	// pods is a map key by a pod's full-name and the value is a pointer to the pod.
	pods    map[string]*v1.Pod
	keyFunc func(*v1.Pod) string
}

// Add adds a pod to the unschedulable pods.
func (u *UnschedulablePodsMap) addOrUpdate(pod *v1.Pod) {
	u.pods[u.keyFunc(pod)] = pod
}

// Delete deletes a pod from the unschedulable pods.
func (u *UnschedulablePodsMap) delete(pod *v1.Pod) {
	delete(u.pods, u.keyFunc(pod))
}

// Get returns the pod if a pod with the same key as the key of the given "pod"
// is found in the map. It returns nil otherwise.
func (u *UnschedulablePodsMap) get(pod *v1.Pod) *v1.Pod {
	podKey := u.keyFunc(pod)
	if p, exists := u.pods[podKey]; exists {
		return p
	}
	return nil
}

// Clear removes all the entries from the unschedulable maps.
func (u *UnschedulablePodsMap) clear() {
	u.pods = make(map[string]*v1.Pod)
}

// newUnschedulablePodsMap initializes a new object of UnschedulablePodsMap.
func newUnschedulablePodsMap() *UnschedulablePodsMap {
	return &UnschedulablePodsMap{
		pods:    make(map[string]*v1.Pod),
		keyFunc: util.GetPodFullName,
	}
}
