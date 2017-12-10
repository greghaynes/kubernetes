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

type clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

// backoffEntry is single threaded.  in particular, it only allows a single action to be waiting on backoff at a time.
// It is not safe to copy this object.
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

func (b *backoffEntry) backoffTime() time.Time {
	return b.lastUpdate.Add(b.backoff)
}

func (entry *backoffEntry) doBackoff(maxDuration time.Duration) time.Duration {
	if !entry.initialized {
		entry.initialized = true
		return entry.backoff
	}
	newDuration := entry.backoff * 2
	if newDuration > maxDuration {
		newDuration = maxDuration
	}
	entry.backoff = newDuration
	glog.V(4).Infof("Backing off %s", newDuration.String())
	return newDuration
}

func (entry *backoffEntry) backoffAndWait(maxDuration time.Duration) {
	time.Sleep(entry.doBackoff(maxDuration))
}

type PodBackoff struct {
	expiryQ         *Heap
	backoffQ        *Heap
	lock            sync.Mutex
	clock           clock
	defaultDuration time.Duration
	maxDuration     time.Duration
}

func (p *PodBackoff) MaxDuration() time.Duration {
	return p.maxDuration
}

func CreateDefaultPodBackoff() *PodBackoff {
	return CreatePodBackoff(1*time.Second, 60*time.Second)
}

func CreatePodBackoff(defaultDuration, maxDuration time.Duration) *PodBackoff {
	return CreatePodBackoffWithClock(defaultDuration, maxDuration, realClock{})
}

func CreatePodBackoffWithClock(defaultDuration, maxDuration time.Duration, clock clock) *PodBackoff {
	return &PodBackoff{
		expiryQ:         NewHeap(backoffEntryKeyFunc, backoffEntryCompareUpdate),
		backoffQ:        NewHeap(backoffEntryKeyFunc, backoffEntryCompareBackoff),
		clock:           clock,
		defaultDuration: defaultDuration,
		maxDuration:     maxDuration,
	}
}

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

func (p *PodBackoff) doBackoff(be *backoffEntry) time.Duration {
	duration := be.doBackoff(p.maxDuration)
	p.backoffQ.Update(be)
	return duration
}

func (p *PodBackoff) BackoffPod(podID ktypes.NamespacedName) time.Duration {
	p.lock.Lock()
	defer p.lock.Unlock()
	entry := p.getEntry(podID)
	entry.lastUpdate = p.clock.Now()
	return p.doBackoff(entry)
}

// TryBackoffAndWait tries to acquire the backoff lock, maxDuration is the maximum allowed period to wait for.
func (p *PodBackoff) TryBackoffAndWait(podID ktypes.NamespacedName) bool {
	p.lock.Lock()
	entry := p.getEntry(podID)

	if !entry.tryLock() {
		p.lock.Unlock()
		return false
	}
	defer entry.unlock()
	duration := p.doBackoff(entry)
	p.lock.Unlock()
	time.Sleep(duration)
	return true
}

func (p *PodBackoff) Gc() {
	p.lock.Lock()
	defer p.lock.Unlock()
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
			p.backoffQ.Delete(entry)
		} else {
			break
		}
	}
}

func backoffEntryKeyFunc(b interface{}) (string, error) {
	be := b.(*backoffEntry)
	return be.podName.String(), nil
}

// Returns true when b1's backoff time is before b2's
func backoffEntryCompareUpdate(b1, b2 interface{}) bool {
	be1 := b1.(*backoffEntry)
	be2 := b2.(*backoffEntry)
	return be1.lastUpdate.Before(be2.lastUpdate)
}

// Returns true when b1's backoff time is before b2's
func backoffEntryCompareBackoff(b1, b2 interface{}) bool {
	be1 := b1.(*backoffEntry)
	be2 := b2.(*backoffEntry)
	return be1.backoffTime().Before(be2.backoffTime())
}
