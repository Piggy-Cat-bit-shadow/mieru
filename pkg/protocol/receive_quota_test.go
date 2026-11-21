// Copyright (C) 2026  mieru authors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package protocol

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"
)

// newReceiveQuotaPair creates a receive pair whose underlay either enforces the
// byte quota (exclusive) or carries sessions without pausing its reader
// (shared).
func newReceiveQuotaPair(t *testing.T, quota int64, exclusive bool) *receivePair {
	t.Helper()
	p := newReceivePair(t)
	p.receiver.exclusive = exclusive
	p.receiver.recvQuota.Store(quota)
	return p
}

// TestReceiveQuotaBackpressureAndResume proves that an exclusive stream
// underlay stops reading at the declared byte quota, that the blocked sender
// resumes when the application reads, and that the byte stream survives the
// pause unchanged.
func TestReceiveQuotaBackpressureAndResume(t *testing.T) {
	const segmentSize = maxPDU
	const payloadSegments = 4
	quota := int64(MinReceiveByteQuota)

	p := newReceiveQuotaPair(t, quota, true)
	session := p.addSession(t, 7)

	payloads := make([][]byte, payloadSegments)
	for i := range payloads {
		payloads[i] = quotaTestPayload(i, segmentSize)
	}
	results := p.writeSegments(7, payloads)

	// Two segments fill the quota exactly.
	waitQuotaWrite(t, results, 0)
	waitQuotaWrite(t, results, 1)
	if got := session.UnreadPayloadBytes(); got != quota {
		t.Fatalf("unread payload bytes = %d, want %d after two segments", got, quota)
	}

	// The receiver must be parked before it reads the third payload, so the
	// third write cannot complete.
	select {
	case result := <-results:
		t.Fatalf("write segment %d completed while the quota was full (err %v)", result.index, result.err)
	case <-time.After(150 * time.Millisecond):
	}

	// Reading exactly one segment returns its bytes to the quota and wakes the
	// parked underlay read.
	received := make([]byte, 0, segmentSize*payloadSegments)
	buf := make([]byte, segmentSize)
	if _, err := io.ReadFull(session, buf); err != nil {
		t.Fatalf("ReadFull() failed: %v", err)
	}
	received = append(received, buf...)
	// The parked third segment is admitted as soon as the read returns bytes
	// to the quota, so the counter is refilled rather than left at the
	// intermediate value.
	waitQuotaWrite(t, results, 2)
	if got := session.UnreadPayloadBytes(); got != quota {
		t.Fatalf("unread payload bytes after resume = %d, want %d", got, quota)
	}

	// Reading the rest lets the last segment through. ReadFull parks until the
	// sender is admitted, which proves both directions make progress.
	rest := make([]byte, segmentSize*(payloadSegments-1))
	if _, err := io.ReadFull(session, rest); err != nil {
		t.Fatalf("ReadFull(rest) failed: %v", err)
	}
	received = append(received, rest...)
	waitQuotaWrite(t, results, 3)

	if got := session.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("unread payload bytes after full read = %d, want 0", got)
	}
	var want []byte
	for _, payload := range payloads {
		want = append(want, payload...)
	}
	if len(received) != len(want) {
		t.Fatalf("received %d bytes, want %d", len(received), len(want))
	}
	if !bytes.Equal(received, want) {
		t.Fatal("received byte sequence differs from the sent byte sequence")
	}
	if sha256.Sum256(received) != sha256.Sum256(want) {
		t.Fatal("received checksum differs from the sent checksum")
	}
}

// TestReceiveQuotaCloseWhileFull proves that Close wakes a parked admission,
// keeps already received data readable, and never leaves the accounting
// negative or permanently stuck.
func TestReceiveQuotaCloseWhileFull(t *testing.T) {
	const segmentSize = maxPDU
	quota := int64(MinReceiveByteQuota)
	p := newReceiveQuotaPair(t, quota, true)
	session := p.addSession(t, 9)

	payloads := [][]byte{
		quotaTestPayload(0, segmentSize),
		quotaTestPayload(1, segmentSize),
		quotaTestPayload(2, segmentSize),
	}
	results := p.writeSegments(9, payloads)
	waitQuotaWrite(t, results, 0)
	waitQuotaWrite(t, results, 1)

	select {
	case result := <-results:
		t.Fatalf("write segment %d completed while the quota was full", result.index)
	case <-time.After(100 * time.Millisecond):
	}

	if err := session.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}
	// The parked third segment must be drained, not delivered: the read side
	// is closed, but the writer must not remain blocked forever.
	waitQuotaWrite(t, results, 2)
	select {
	case <-session.closedChan:
	case <-time.After(time.Second):
		t.Fatal("session did not reach the closed state")
	}
	if got := session.UnreadPayloadBytes(); got != quota {
		t.Fatalf("unread payload bytes after close = %d, want the still readable %d", got, quota)
	}

	// The two delivered segments stay readable after Close.
	received := make([]byte, quota)
	if _, err := io.ReadFull(session, received); err != nil {
		t.Fatalf("ReadFull() after close failed: %v", err)
	}
	var want []byte
	want = append(want, payloads[0]...)
	want = append(want, payloads[1]...)
	if !bytes.Equal(received, want) {
		t.Fatal("buffered payload changed across Close")
	}
	if got := session.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("unread payload bytes after draining a closed session = %d, want 0", got)
	}
	if _, err := session.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("Read() on drained closed session = %v, want io.EOF", err)
	}
}

// TestReceiveQuotaContextCancelWhileFull proves that cancelling the underlay
// context releases a parked admission and terminates the event loop.
func TestReceiveQuotaContextCancelWhileFull(t *testing.T) {
	const segmentSize = maxPDU
	quota := int64(MinReceiveByteQuota)
	p := newReceiveQuotaPair(t, quota, true)
	session := p.addSession(t, 10)

	payloads := [][]byte{
		quotaTestPayload(0, segmentSize),
		quotaTestPayload(1, segmentSize),
		quotaTestPayload(2, segmentSize),
	}
	results := p.writeSegments(10, payloads)
	waitQuotaWrite(t, results, 0)
	waitQuotaWrite(t, results, 1)
	select {
	case result := <-results:
		t.Fatalf("write segment %d completed while the quota was full", result.index)
	case <-time.After(100 * time.Millisecond):
	}

	p.cancel()
	select {
	case <-p.loopExited:
	case <-time.After(2 * time.Second):
		t.Fatal("RunEventLoop() did not stop after context cancellation")
	}
	// Closing the event loop closes the transport, so the blocked writer must
	// observe an error instead of hanging.
	select {
	case result := <-results:
		if result.err == nil && result.index == 2 {
			t.Fatalf("write segment 2 unexpectedly succeeded after cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked writer did not exit after context cancellation")
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}
	if got := session.UnreadPayloadBytes(); got != quota {
		t.Fatalf("unread payload bytes after cancel = %d, want %d", got, quota)
	}
}

// TestReceiveQuotaSharedUnderlayNotBlocked proves that a byte quota attached to
// a shared underlay is never used as backpressure: a slow reader must not stall
// another session, and closing the slow session must not close the underlay.
func TestReceiveQuotaSharedUnderlayNotBlocked(t *testing.T) {
	const segmentSize = maxPDU
	quota := int64(MinReceiveByteQuota)
	p := newReceiveQuotaPair(t, quota, false)
	slow := p.addSession(t, 21)
	fast := p.addSession(t, 22)

	// More than the quota is written to the slow session and is intentionally
	// never read. A shared underlay must keep reading.
	slowPayloads := make([][]byte, 4)
	for i := range slowPayloads {
		slowPayloads[i] = quotaTestPayload(i, segmentSize)
	}
	results := p.writeSegments(21, slowPayloads)
	for i := range slowPayloads {
		waitQuotaWrite(t, results, i)
	}
	if got := slow.UnreadPayloadBytes(); got <= quota {
		t.Fatalf("slow session unread payload bytes = %d, want more than quota %d on a shared underlay", got, quota)
	}

	// The fast session must still make progress while the slow reader is
	// parked.
	fastPayload := quotaTestPayload(99, 4096)
	fastResults := p.writeSegments(22, [][]byte{fastPayload})
	waitQuotaWrite(t, fastResults, 0)
	got := make([]byte, len(fastPayload))
	if _, err := io.ReadFull(fast, got); err != nil {
		t.Fatalf("ReadFull(fast) failed: %v", err)
	}
	if !bytes.Equal(got, fastPayload) {
		t.Fatal("fast session payload differs")
	}

	// Closing the slow session must not close the shared underlay.
	if err := slow.Close(); err != nil {
		t.Fatalf("slow.Close() failed: %v", err)
	}
	if err := p.receiver.RemoveSession(slow); err != nil {
		t.Fatalf("RemoveSession(slow) failed: %v", err)
	}
	if got := p.receiver.SessionCount(); got != 1 {
		t.Fatalf("session count after slow removal = %d, want 1", got)
	}
	select {
	case <-p.receiver.Done():
		t.Fatal("shared underlay was closed when one session closed")
	default:
	}
	againPayload := quotaTestPayload(100, 2048)
	again := p.writeSegments(22, [][]byte{againPayload})
	waitQuotaWrite(t, again, 0)
	got = make([]byte, len(againPayload))
	if _, err := io.ReadFull(fast, got); err != nil {
		t.Fatalf("ReadFull(fast) after slow close failed: %v", err)
	}
	if !bytes.Equal(got, againPayload) {
		t.Fatal("fast session payload after slow close differs")
	}
}

// TestReceiveQuotaRepeatedRoundsNoLeak runs repeated open / slow read / close
// rounds and checks that goroutines and live heap do not grow monotonically.
func TestReceiveQuotaRepeatedRoundsNoLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("leak check is slow in -short mode")
	}
	const rounds = 25
	const segmentSize = 4096
	const quota = int64(MinReceiveByteQuota)

	baselineGoroutines := runtime.NumGoroutine()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for round := 0; round < rounds; round++ {
		p := newReceiveQuotaPair(t, quota, true)
		session := p.addSession(t, uint32(1000+round))
		payloads := [][]byte{quotaTestPayload(round, segmentSize), quotaTestPayload(round+1, segmentSize)}
		results := p.writeSegments(uint32(1000+round), payloads)
		waitQuotaWrite(t, results, 0)
		waitQuotaWrite(t, results, 1)
		if _, err := io.ReadFull(session, make([]byte, 10)); err != nil {
			t.Fatalf("round %d ReadFull() failed: %v", round, err)
		}
		if err := session.Close(); err != nil {
			t.Fatalf("round %d Close() failed: %v", round, err)
		}
		p.close()
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		if runtime.NumGoroutine() <= baselineGoroutines+2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baselineGoroutines+2 {
		t.Errorf("goroutine count after %d rounds = %d, baseline %d", rounds, got, baselineGoroutines)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	t.Logf("goroutines baseline=%d final=%d; heap before=%d after=%d", baselineGoroutines, runtime.NumGoroutine(), before.HeapAlloc, after.HeapAlloc)
	const maxGrowth = 8 << 20
	if after.HeapAlloc > before.HeapAlloc+maxGrowth {
		t.Errorf("live heap after %d rounds = %d, before = %d", rounds, after.HeapAlloc, before.HeapAlloc)
	}
}

// TestReceiveQuotaMinimumAdmitsMaximumSegment proves that the configured floor
// admits a single maximum-size payload and that strict admission keeps the
// counter at the quota.
func TestReceiveQuotaMinimumAdmitsMaximumSegment(t *testing.T) {
	quota := int64(MinReceiveByteQuota)
	if quota <= int64(^uint16(0)) {
		t.Fatalf("MinReceiveByteQuota %d does not admit a maximum uint16 payload", quota)
	}
	if DefaultReceiveByteQuota < MinReceiveByteQuota {
		t.Fatalf("DefaultReceiveByteQuota %d is below MinReceiveByteQuota %d", DefaultReceiveByteQuota, MinReceiveByteQuota)
	}
	p := newReceiveQuotaPair(t, quota, true)
	session := p.addSession(t, 51)

	// 65535 is the largest plaintext payload a dataAckStruct can declare.
	const maxPlaintext = int(^uint16(0))
	payload := quotaTestPayload(0, maxPlaintext)
	results := p.writeSegments(51, [][]byte{payload})
	waitQuotaWrite(t, results, 0)
	if got := session.UnreadPayloadBytes(); got != int64(maxPlaintext) {
		t.Fatalf("unread payload bytes = %d, want %d", got, maxPlaintext)
	}
	got := make([]byte, maxPlaintext)
	if _, err := io.ReadFull(session, got); err != nil {
		t.Fatalf("ReadFull() failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("maximum segment payload differs")
	}
	if got := session.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("unread payload bytes after read = %d, want 0", got)
	}
}
