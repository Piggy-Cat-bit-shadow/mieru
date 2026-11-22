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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

package protocol

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

// TestSharedDeliverClosesOnlyTheOverflowedSession is the deterministic unit
// test of the shared-underlay overload policy. With the bounded delivery queue
// full, a delivery must return without parking the reader, close the overflowed
// session exactly once, and leave a second session on the same underlay
// deliverable. The alternative policy - waiting for queue space - is what the
// shared single reader must never do, because it stalls every other session.
func TestSharedDeliverClosesOnlyTheOverflowedSession(t *testing.T) {
	underlay := newBaseUnderlay(true, 1400, nil)
	slow := NewSession(101, true, 1400, nil, nil)
	if err := underlay.AddSession(slow, nil); err != nil {
		t.Fatalf("AddSession(slow) failed: %v", err)
	}
	// Fill the delivery channel without starting the input loop, so the next
	// delivery meets exactly the state a stopped application produces.
	for i := 0; i < segmentChanCapacity; i++ {
		slow.recvChan <- quotaTestSegment(101, uint32(i), nil)
	}

	overflow := quotaTestSegment(101, segmentChanCapacity, []byte("overflow"))
	done := make(chan bool, 1)
	go func() {
		done <- underlay.deliverSegmentToSession(slow, overflow)
	}()
	select {
	case delivered := <-done:
		if delivered {
			t.Fatal("deliverSegmentToSession() accepted a segment into a full shared queue")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deliverSegmentToSession() blocked the reader on a shared underlay; one slow session must not stall the other sessions")
	}
	if got := slow.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("unread payload bytes after a refused delivery = %d, want 0", got)
	}

	// The overflowed session is closed and its bounded queue is drained.
	select {
	case <-slow.closedChan:
	case <-time.After(2 * time.Second):
		t.Fatal("the overflowed session was not closed")
	}
	if got := len(slow.recvChan); got != 0 {
		t.Fatalf("recvChan length after the overflow close = %d, want 0", got)
	}
	if got := slow.recvQueue.Len(); got != 0 {
		t.Fatalf("recvQueue length after the overflow close = %d, want 0", got)
	}

	// A second session on the same shared underlay is unaffected.
	fast := NewSession(102, true, 1400, nil, nil)
	if err := underlay.AddSession(fast, nil); err != nil {
		t.Fatalf("AddSession(fast) failed: %v", err)
	}
	if !underlay.deliverSegmentToSession(fast, quotaTestSegment(102, 0, []byte("still moving"))) {
		t.Fatal("deliverSegmentToSession() rejected a segment for a healthy session on the same underlay")
	}
	if got := len(fast.recvChan); got != 1 {
		t.Fatalf("fast session recvChan length = %d, want 1", got)
	}
	select {
	case <-underlay.Done():
		t.Fatal("the shared underlay was closed when one session overflowed")
	default:
	}
}

// TestSharedUnderlayOverflowFailsOnlyTheSlowSession drives a real in-memory
// stream underlay carrying two sessions. One application stops reading, its
// bounded receive queue overflows, and the test proves the whole policy: the
// other session keeps making byte progress throughout, the shared underlay and
// its reader stay healthy, the overflowed session is closed promptly, only its
// already queued prefix stays readable, and its accounting returns to zero.
func TestSharedUnderlayOverflowFailsOnlyTheSlowSession(t *testing.T) {
	const slowSessionID = 201
	const fastSessionID = 202
	const slowPayloadSize = 1024
	// More in-order segments than the receive queue plus the delivery channel
	// can hold, so at least one delivery meets a genuinely full queue.
	const slowSegments = segmentTreeCapacity + segmentChanCapacity + 16

	p := newReceivePair(t)
	// The underlay is shared: one reader for both sessions, and no byte quota,
	// exactly like the mux configures a multiplexed connection.
	p.receiver.exclusive = false
	p.receiver.recvQuota.Store(0)

	slow := p.addSession(t, slowSessionID)
	fast := p.addSession(t, fastSessionID)

	slowPayload := quotaTestPayload(0, slowPayloadSize)
	slowWriteDone := make(chan error, 1)
	go func() {
		for i := 0; i < slowSegments; i++ {
			if err := p.sender.writeOneSegment(quotaTestSegment(slowSessionID, uint32(i), slowPayload)); err != nil {
				slowWriteDone <- err
				return
			}
		}
		slowWriteDone <- nil
	}()

	// The fast session must keep making byte progress while the slow session's
	// queue fills and overflows: that is the head-of-line-blocking regression.
	fastDone := make(chan error, 1)
	go func() {
		for round := 0; round < 20; round++ {
			payload := quotaTestPayload(round, 512)
			if err := p.sender.writeOneSegment(quotaTestSegment(fastSessionID, uint32(round), payload)); err != nil {
				fastDone <- fmt.Errorf("round %d write: %w", round, err)
				return
			}
			if err := fast.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
				fastDone <- fmt.Errorf("round %d deadline: %w", round, err)
				return
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(fast, got); err != nil {
				fastDone <- fmt.Errorf("round %d read: %w", round, err)
				return
			}
			if string(got) != string(payload) {
				fastDone <- fmt.Errorf("round %d payload differs", round)
				return
			}
		}
		fastDone <- nil
	}()

	select {
	case <-slow.closedChan:
	case <-time.After(15 * time.Second):
		t.Fatal("the slow session was not closed after its bounded queue filled; the shared reader is still blocked on one session")
	}
	// The reader keeps consuming the stream even after the session is closed,
	// so the peer's writes still complete instead of hanging on the connection.
	select {
	case err := <-slowWriteDone:
		if err != nil {
			t.Fatalf("writing the slow session failed: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the shared reader stopped consuming the connection after the session overflowed")
	}
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatalf("the fast session made no progress while the slow session's queue was full: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the fast session made no progress while the slow session's queue was full")
	}

	// The overflowed session stopped accepting within its bounded backlog.
	slow.wg.Wait()
	if got := len(slow.recvChan); got != 0 {
		t.Fatalf("slow session recvChan length after the overflow close = %d, want 0", got)
	}
	if got := slow.recvBuf.Len(); got != 0 {
		t.Fatalf("slow session recvBuf length after the overflow close = %d, want 0", got)
	}
	queued := slow.recvQueue.Len()
	if queued > segmentTreeCapacity {
		t.Fatalf("slow session recvQueue length after the overflow close = %d, want at most %d", queued, segmentTreeCapacity)
	}
	wantGauge := int64(queued) * slowPayloadSize
	if got := slow.UnreadPayloadBytes(); got != wantGauge {
		t.Fatalf("slow session unread payload bytes after the overflow close = %d, want the %d queued bytes", got, wantGauge)
	}
	t.Logf("slow session failed with %d queued segments (%d payload bytes) out of %d segments written; bounded by %d + %d queue slots",
		queued, wantGauge, slowSegments, segmentTreeCapacity, segmentChanCapacity)

	// The already queued payload is still readable, in order, and then reads
	// end: the session must fail instead of parking its reader forever.
	if err := slow.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() failed: %v", err)
	}
	buffer := make([]byte, slowPayloadSize)
	var consumed int64
	for {
		n, err := slow.Read(buffer)
		for i := 0; i < n; i++ {
			if want := slowPayload[(consumed+int64(i))%int64(len(slowPayload))]; buffer[i] != want {
				t.Fatalf("byte %d read from the overflowed session is wrong", consumed+int64(i))
			}
		}
		consumed += int64(n)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("reading the overflowed session ended with %v, want io.EOF", err)
			}
			break
		}
	}
	if consumed != wantGauge {
		t.Fatalf("overflowered session delivered %d bytes, want its %d queued bytes", consumed, wantGauge)
	}
	if consumed >= int64(slowSegments)*slowPayloadSize {
		t.Fatalf("overflowered session delivered its whole stream (%d bytes); it was not failed", consumed)
	}
	if got := slow.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("slow session unread payload bytes after draining = %d, want 0", got)
	}

	// The shared underlay and the other session survive the failure.
	select {
	case <-p.receiver.Done():
		t.Fatal("the shared underlay was closed when one session overflowed")
	default:
	}
	payload := quotaTestPayload(99, 512)
	if err := p.sender.writeOneSegment(quotaTestSegment(fastSessionID, 20, payload)); err != nil {
		t.Fatalf("writing the fast session after the slow close failed: %v", err)
	}
	if err := fast.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() failed: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(fast, got); err != nil {
		t.Fatalf("reading the fast session after the slow close failed: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatal("fast session payload after the slow close differs")
	}
}
