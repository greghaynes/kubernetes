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

package util

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	ktypes "k8s.io/apimachinery/pkg/types"

	"github.com/golang/glog"
)

// backoffEntry is single threaded.  in particular, it only allows a single action to be waiting on backoff at a time.
// It is also not safe to copy this object.
type backoffEntry struct {
	initialized bool
	podName     ktypes.NamespacedName
	backoff     time.Duration
	lastUpdate  time.Time
	reqInFlight int32
}

// tryLock attempts to acquire a lock via atomic compare and swap.
// returns true if the lock was acquired, false otherwise
func (b *backoffEntry) tryLock() bool {
	return atomic.CompareAndSwapInt32(&b.reqInFlight, 0, 1)
}

// unlock returns the lock.  panics if the lock isn't held
func (b *backoffEntry) unlock() {
	if !atomic.CompareAndSwapInt32(&b.reqInFlight, 1, 0) {
		panic(fmt.Sprintf("unexpected state on unlocking: %+v", b))
	}
}

// backoffTime returns the Time when a backoffEntry completes backoff
func (b *backoffEntry) backoffTime() time.Time {
	return b.lastUpdate.Add(b.backoff)
}

// getBackoff returns the duration until this entry completes backoff
func (b *backoffEntry) getBackoff(maxDuration time.Duration) time.Duration {
	if !b.initialized {
		b.initialized = true
		return b.backoff
	}
	newDuration := b.backoff * 2
	if newDuration > maxDuration {
		newDuration = maxDuration
	}
	b.backoff = newDuration
	glog.V(4).Infof("Backing off %s", newDuration.String())
	return newDuration
}

// backoffAndWait Blocks until this entry has completed backoff
func (b *backoffEntry) backoffAndWait(maxDuration time.Duration) {
	time.Sleep(b.getBackoff(maxDuration))
}

// PodBackoff is used to restart a pod with back-off delay.
type PodBackoff struct {
	// expiryQ stores backoffEntry orderedy by lastUpdate until they reach maxDuration and are GC'd
	expiryQ *Heap
	// backoffQ stores backoffEntry ordered by backoff time until they are removed in PopBackoffCompleted
	backoffQ        *Heap
	boUpdated       chan struct{}
	lock            sync.Mutex
	clock           Clock
	defaultDuration time.Duration
	maxDuration     time.Duration
}

// MaxDuration returns the max time duration of the back-off.
func (p *PodBackoff) MaxDuration() time.Duration {
	return p.maxDuration
}

// CreateDefaultPodBackoff creates a default pod back-off object.
func CreateDefaultPodBackoff() *PodBackoff {
	return CreateDefaultPodBackoffWithClock(RealClock{})
}

// CreateDefaultPodBackoffWithClock creates a default pod back-off object with the passed clock.
func CreateDefaultPodBackoffWithClock(clock Clock) *PodBackoff {
	return CreatePodBackoffWithClock(1*time.Second, 60*time.Second, clock)
}

// CreatePodBackoff creates a pod back-off object by default duration and max duration.
func CreatePodBackoff(defaultDuration, maxDuration time.Duration) *PodBackoff {
	return CreatePodBackoffWithClock(defaultDuration, maxDuration, RealClock{})
}

// CreatePodBackoffWithClock creates a pod back-off object by default duration, max duration and clock.
func CreatePodBackoffWithClock(defaultDuration, maxDuration time.Duration, clock Clock) *PodBackoff {
	p := PodBackoff{
		expiryQ:         NewHeap(backoffEntryKeyFunc, backoffEntryCompareUpdate),
		backoffQ:        NewHeap(backoffEntryKeyFunc, backoffEntryCompareBackoff),
		clock:           clock,
		defaultDuration: defaultDuration,
		maxDuration:     maxDuration,
	}
	p.boUpdated = make(chan struct{})
	return &p
}

// getEntry returns the backoffEntry for a given podID
func (p *PodBackoff) getEntry(podID ktypes.NamespacedName) *backoffEntry {
	entry, exists, _ := p.expiryQ.GetByKey(podID.String())
	var be *backoffEntry
	if !exists {
		be = &backoffEntry{
			initialized: false,
			podName:     podID,
			backoff:     p.defaultDuration,
		}
		p.expiryQ.Update(be)
	} else {
		be = entry.(*backoffEntry)
	}
	return be
}

func (p *PodBackoff) boUpdateBroadcast() {
	close(p.boUpdated)
	p.boUpdated = make(chan struct{})
}

// getBackoff updates the backoff for an entry and returns the duration until backoff completion
func (p *PodBackoff) getBackoff(be *backoffEntry) time.Duration {
	duration := be.getBackoff(p.maxDuration)
	p.backoffQ.Update(be)
	p.boUpdateBroadcast()
	return duration
}

// IsPodBackingOff returns whether a pod is currently in the backoff queue
func (p *PodBackoff) IsPodBackingOff(podID ktypes.NamespacedName) bool {
	_, exists, _ := p.backoffQ.GetByKey(podID.String())
	return exists
}

// BackoffPod updates the backoff for a podId and returns the duration until backoff completion
func (p *PodBackoff) BackoffPod(podID ktypes.NamespacedName) time.Duration {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.gc()
	entry := p.getEntry(podID)
	entry.lastUpdate = p.clock.Now()
	p.expiryQ.Update(entry)
	return p.getBackoff(entry)
}

// CancelPodBackoff removes a pod from the backoff queue
func (p *PodBackoff) CancelPodBackoff(podID ktypes.NamespacedName) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	entry, exists, _ := p.expiryQ.GetByKey(podID.String())
	if exists == false {
		return false
	}

	err := p.backoffQ.Delete(entry)
	p.boUpdateBroadcast()
	if err != nil {
		return false
	}
	return true
}

// gc execute garbage collection on the pod back-off.
func (p *PodBackoff) gc() {
	now := p.clock.Now()
	var be *backoffEntry
	for {
		entry := p.expiryQ.Peek()
		if entry == nil {
			break
		}
		be = entry.(*backoffEntry)
		if now.Sub(be.lastUpdate) > p.maxDuration {
			p.expiryQ.Pop()
		} else {
			break
		}
	}
}

// PopCompletedBackoff blocks until a pod has completed backoff and then returns that pod name
func (p *PodBackoff) PopCompletedBackoff(stop <-chan struct{}) (bool, ktypes.NamespacedName) {
	for {
		p.lock.Lock()
		b := p.backoffQ.Peek()
		if b == nil {
			// No pods being backed off
			p.lock.Unlock()
			select {
			case <-stop:
				return false, ktypes.NamespacedName{}
			case <-p.boUpdated:
			}
			continue
		}
		be := b.(*backoffEntry)
		if be.backoffTime().Before(p.clock.Now()) {
			// Pod has completed backoff
			p.backoffQ.Pop()
			p.boUpdateBroadcast()
			p.lock.Unlock()
			return true, be.podName
		}

		// Wait for change in backoffQ or completion of be backoff
		boTime := be.backoffTime()
		boDur := time.After(boTime.Sub(p.clock.Now()))

		p.lock.Unlock()
		select {
		case <-stop:
			return false, ktypes.NamespacedName{}
		case <-boDur:
		case <-p.boUpdated:
		}
	}
}

// backoffEntryKeyFunc is the keying function used for mapping a backoffEntry to string for heap
func backoffEntryKeyFunc(b interface{}) (string, error) {
	be := b.(*backoffEntry)
	return be.podName.String(), nil
}

// backoffEntryCompareUpdate returns true when b1's backoff time is before b2's
func backoffEntryCompareUpdate(b1, b2 interface{}) bool {
	be1 := b1.(*backoffEntry)
	be2 := b2.(*backoffEntry)
	return be1.lastUpdate.Before(be2.lastUpdate)
}

// backoffEntryCompareBackoff returns true when b1's backoff time is before b2's
func backoffEntryCompareBackoff(b1, b2 interface{}) bool {
	be1 := b1.(*backoffEntry)
	be2 := b2.(*backoffEntry)
	return be1.backoffTime().Before(be2.backoffTime())
}
