// Copyright (c) 2014-2019 Appsicle
// Copyright (c) 2019-2026 QuestDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package questdb

import (
	"errors"
	"runtime/debug"
	"sync"
)

var qwpFailedDrainers struct {
	sync.Mutex
	items []*qwpSfOrphanDrainer
}
var qwpFailedDrainerPools struct {
	sync.Mutex
	items []*qwpSfDrainerPool
}

func (p *qwpSfDrainerPool) failTask(d *qwpSfOrphanDrainer, cause any) {
	err := &qwpCleanupPanicError{phase: "drainer task", cause: cause, stack: debug.Stack()}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed.Store(true)
	p.cancel()
	p.cleanupErr = errors.Join(p.cleanupErr, err)
	p.failed = append(p.failed, d)
	p.retainFailureLocked()
}

func (p *qwpSfDrainerPool) retainFailureLocked() {
	qwpFailedDrainerPools.Lock()
	defer qwpFailedDrainerPools.Unlock()
	for _, held := range qwpFailedDrainerPools.items {
		if held == p {
			return
		}
	}
	qwpFailedDrainerPools.items = append(qwpFailedDrainerPools.items, p)
}

func (d *qwpSfOrphanDrainer) listenerDispatcher() *qwpDispatcher[func()] {
	d.notificationsOnce.Do(func() {
		d.notifications = newQwpDispatcher(func(fn func()) { qwpDrainerListenerCall(d.logger, fn) },
			func(func()) string { return "orphan drainer notification" }, nil, "qwp/sf drainer", 64)
		d.notifications.logger = d.logger
	})
	return d.notifications
}
func (d *qwpSfOrphanDrainer) notifyListener(fn func()) { d.listenerDispatcher().offer(fn) }

func (d *qwpSfOrphanDrainer) cleanupResult() error {
	d.cleanupMu.Lock()
	defer d.cleanupMu.Unlock()
	result := d.cleanupErr
	if d.cleanup != nil {
		result = errors.Join(result, d.cleanup.cleanupResult())
		if !d.cleanup.engineCloseCompleted() {
			result = errors.Join(result, ErrCleanupPending, ErrSfCleanupPending)
		}
	}
	return result
}

// Wait for the engine to report cleanup progress, rather than repeatedly
// checking a blocked reader or waiting for logging to finish. If the drainer
// has already failed permanently, return without waiting for engine shutdown;
// its resources remain held.
func (d *qwpSfOrphanDrainer) waitCleanup() {
	d.cleanupMu.Lock()
	e, err := d.cleanup, d.cleanupErr
	d.cleanupMu.Unlock()
	if e == nil || errors.Is(err, ErrCleanupFailed) {
		return
	}
	for {
		e.cleanup.mu.Lock()
		finished, changed := e.cleanup.finished, e.cleanup.changed
		e.cleanup.mu.Unlock()
		if finished {
			return
		}
		<-changed
	}
}

func (p *qwpSfDrainerPool) cleanupResult() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := p.cleanupErr
	if errors.Is(result, ErrCleanupFailed) {
		if len(p.active) > 0 {
			result = errors.Join(result, ErrCleanupPending, ErrSfCleanupPending)
		}
		return result
	}
	for _, d := range p.active {
		result = errors.Join(result, d.cleanupResult())
	}
	if len(p.active) > 0 || !p.closed.Load() {
		result = errors.Join(result, ErrCleanupPending, ErrSfCleanupPending)
	}
	return result
}
func (p *qwpSfDrainerPool) cleanupCompleted() bool {
	result := p.cleanupResult()
	return !errors.Is(result, ErrCleanupPending) && !errors.Is(result, ErrCleanupFailed)
}
