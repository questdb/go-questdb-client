/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package questdb

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"sync/atomic"
)

// qwpSfAcquiredResources keeps files and mappings opened before construction or
// recovery failed. The engine takes responsibility for these resources before
// returning the error. This struct only holds references; it runs no cleanup
// worker of its own.
type qwpSfAcquiredResources struct {
	segments       []*qwpSfSegment
	manifests      []*qwpSfManifest
	watermarks     []*qwpSfAckWatermark
	mappingObjects []*qwpSfMappingObject
}

// Windows creates a mapping handle before mapping the file into memory. If
// that second step fails, the handle still needs closing. There is no memory
// address to unmap in this case, and no equivalent separate handle on Unix.
type qwpSfMappingObject struct{ handle uintptr }

func (m *qwpSfMappingObject) close() error { return qwpSfCloseMappingObject(m) }

func (b *qwpSfAcquiredResources) released() bool {
	if b == nil {
		return true
	}
	for _, s := range b.segments {
		if !s.resourcesReleased() {
			return false
		}
	}
	for _, m := range b.manifests {
		if m != nil && m.file != nil {
			return false
		}
	}
	for _, w := range b.watermarks {
		if w != nil && (w.buf != nil || w.file != nil) {
			return false
		}
	}
	for _, m := range b.mappingObjects {
		if m != nil && m.handle != 0 {
			return false
		}
	}
	return true
}
func (b *qwpSfAcquiredResources) close() error { return b.closeExcept(nil) }
func (b *qwpSfAcquiredResources) closeExcept(prior *qwpSfAcquiredResources) error {
	if b == nil {
		return nil
	}
	var err error
	for _, s := range b.segments {
		if prior != nil && slices.Contains(prior.segments, s) {
			continue
		}
		err = errors.Join(err, qwpRunCleanupPhaseGuarded("acquired segment", s.close))
	}
	for _, m := range b.manifests {
		if prior != nil && slices.Contains(prior.manifests, m) {
			continue
		}
		err = errors.Join(err, qwpRunCleanupPhaseGuarded("acquired manifest", m.close))
	}
	for _, w := range b.watermarks {
		if prior != nil && slices.Contains(prior.watermarks, w) {
			continue
		}
		err = errors.Join(err, qwpRunCleanupPhaseGuarded("acquired watermark", w.close))
	}
	for _, m := range b.mappingObjects {
		if prior != nil && slices.Contains(prior.mappingObjects, m) {
			continue
		}
		err = errors.Join(err, qwpRunCleanupPhaseGuarded("mapping object", m.close))
	}
	return err
}
func (b *qwpSfAcquiredResources) merge(other *qwpSfAcquiredResources) {
	if other == nil || b == other {
		return
	}
	for _, s := range other.segments {
		found := false
		for _, v := range b.segments {
			if v == s {
				found = true
				break
			}
		}
		if !found {
			b.segments = append(b.segments, s)
		}
	}
	for _, m := range other.manifests {
		found := false
		for _, v := range b.manifests {
			if v == m {
				found = true
				break
			}
		}
		if !found {
			b.manifests = append(b.manifests, m)
		}
	}
	for _, w := range other.watermarks {
		found := false
		for _, v := range b.watermarks {
			if v == w {
				found = true
				break
			}
		}
		if !found {
			b.watermarks = append(b.watermarks, w)
		}
	}
	for _, m := range other.mappingObjects {
		if !slices.Contains(b.mappingObjects, m) {
			b.mappingObjects = append(b.mappingObjects, m)
		}
	}
}

// qwpSfAcquisitionError is an open that stopped while files or mappings it
// acquired are still open. original is the failure that stopped the open.
// cause is the unfinished close. Both are visible to errors.Is. The open loop
// leaves the slot where it is until those resources are released.
type qwpSfAcquisitionError struct {
	original  error
	cause     error
	resources *qwpSfAcquiredResources
}

func (e *qwpSfAcquisitionError) Error() string {
	return fmt.Sprintf("qwp/sf: acquisition failed (%v); resource release incomplete: %v", e.original, e.cause)
}
func (e *qwpSfAcquisitionError) Unwrap() []error {
	switch {
	case e.original != nil && e.cause != nil:
		return []error{e.original, e.cause}
	case e.original != nil:
		return []error{e.original}
	case e.cause != nil:
		return []error{e.cause}
	default:
		return nil
	}
}

func qwpSfFailedAcquisition(original error, b *qwpSfAcquiredResources) error {
	var held *qwpSfAcquisitionError
	errors.As(original, &held)
	// Do not retry cleanup after an internal failure, even while handling a
	// construction error.
	if errors.Is(original, ErrCleanupFailed) {
		if held != nil {
			b.merge(held.resources)
		}
		return &qwpSfAcquisitionError{original: original, cause: ErrCleanupFailed, resources: b}
	}
	var prior *qwpSfAcquiredResources
	if held != nil {
		prior = held.resources
	}
	cleanupErr := b.closeExcept(prior)
	// A called function already tried to release these resources. Leave its
	// unfinished cleanup for the engine worker rather than trying again here.
	if held != nil {
		b.merge(held.resources)
		cleanupErr = errors.Join(cleanupErr, held.cause)
	}
	if b.released() && cleanupErr == nil {
		return original
	}
	return &qwpSfAcquisitionError{original: original, cause: errors.Join(ErrSfDurability, cleanupErr), resources: b}
}

// A panic may pass through several functions that opened resources. Each adds
// its resources here so the engine can keep them all when it catches the panic.
type qwpSfAcquisitionPanic struct {
	cause     any
	resources *qwpSfAcquiredResources
}

func (p *qwpSfAcquisitionPanic) Error() string {
	return fmt.Sprintf("qwp/sf: acquisition panicked: %v", p.cause)
}
func qwpSfRetainAcquisitionPanic(r any, b *qwpSfAcquiredResources) {
	if prior, ok := r.(*qwpSfAcquisitionPanic); ok {
		b.merge(prior.resources)
	}
	panic(&qwpSfAcquisitionPanic{cause: r, resources: b})
}

var qwpSfTestAfterFileCloseHook atomic.Pointer[func(*os.File) error]

func qwpSfCloseFile(f *os.File) error {
	err := f.Close()
	if hook := qwpSfTestAfterFileCloseHook.Load(); hook != nil {
		err = errors.Join(err, (*hook)(f))
	}
	return err
}
