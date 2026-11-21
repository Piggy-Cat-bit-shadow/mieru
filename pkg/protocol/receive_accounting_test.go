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
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/enfein/mieru/v3/pkg/appctl/appctlpb"
	"github.com/enfein/mieru/v3/pkg/cipher"
	"github.com/enfein/mieru/v3/pkg/common"
)

// receivePair is a deterministic in-memory stream underlay pair. The receiver
// underlay carries one or more client sessions; the sender underlay is only
// used to write encoded data segments, so the test controls exactly when each
// segment reaches the receiver. A drain goroutine consumes the receiver's close
// requests so a session Close never blocks on the pipe.
type receivePair struct {
	sender   *StreamUnderlay
	receiver *StreamUnderlay

	senderConn   net.Conn
	receiverConn net.Conn

	ctx        context.Context
	cancel     context.CancelFunc
	loopDone   chan error
	loopExited chan struct{}
	drained    chan struct{}
	closed     sync.Once
}

func newReceivePair(t *testing.T) *receivePair {
	t.Helper()
	senderConn, receiverConn := net.Pipe()
	block, err := cipher.BlockCipherFromPassword([]byte("receive-accounting-test"), false)
	if err != nil {
		t.Fatalf("BlockCipherFromPassword() failed: %v", err)
	}
	block.SetBlockContext(cipher.BlockContext{UserName: "receive-accounting-test"})
	sender := &StreamUnderlay{
		baseUnderlay:       *newBaseUnderlay(true, 1400, nil),
		conn:               senderConn,
		block:              block,
		sessionCleanTicker: time.NewTicker(time.Hour),
	}
	receiver := &StreamUnderlay{
		baseUnderlay:       *newBaseUnderlay(true, 1400, nil),
		conn:               receiverConn,
		block:              block.Clone(),
		sessionCleanTicker: time.NewTicker(time.Hour),
	}
	p := &receivePair{
		sender:       sender,
		receiver:     receiver,
		senderConn:   senderConn,
		receiverConn: receiverConn,
		loopDone:     make(chan error, 1),
		loopExited:   make(chan struct{}),
		drained:      make(chan struct{}),
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	go func() {
		p.loopDone <- receiver.RunEventLoop(p.ctx)
		close(p.loopExited)
	}()
	go func() {
		_, _ = io.Copy(io.Discard, senderConn)
		close(p.drained)
	}()
	t.Cleanup(p.close)
	return p
}

func (p *receivePair) addSession(t *testing.T, id uint32) *Session {
	t.Helper()
	session := NewSession(id, true, p.receiver.MTU(), nil, nil)
	if err := p.receiver.AddSession(session, nil); err != nil {
		t.Fatalf("AddSession() failed: %v", err)
	}
	return session
}

func (p *receivePair) close() {
	p.closed.Do(func() {
		p.cancel()
		p.receiver.Close()
		p.sender.Close()
		_ = p.receiverConn.Close()
		_ = p.senderConn.Close()
		select {
		case <-p.loopExited:
		case <-time.After(2 * time.Second):
		}
		select {
		case <-p.drained:
		case <-time.After(2 * time.Second):
		}
	})
}

// quotaTestPayload returns deterministic payload bytes for one segment.
func quotaTestPayload(segmentIndex int, size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(segmentIndex*31 + i)
	}
	return payload
}

func quotaTestSegment(sessionID uint32, seq uint32, payload []byte) *segment {
	return &segment{
		metadata: &dataAckStruct{
			baseStruct: baseStruct{protocol: uint8(dataServerToClient)},
			sessionID:  sessionID,
			seq:        seq,
			windowSize: uint16(segmentTreeCapacity),
			payloadLen: uint16(len(payload)),
		},
		payload:   payload,
		transport: common.StreamTransport,
	}
}

type quotaWriteResult struct {
	index int
	err   error
}

// writeSegments sends segments synchronously in the given order and reports
// each completed segment index on the returned channel.
func (p *receivePair) writeSegments(sessionID uint32, payloads [][]byte) <-chan quotaWriteResult {
	results := make(chan quotaWriteResult, len(payloads))
	go func() {
		for i, payload := range payloads {
			err := p.sender.writeOneSegment(quotaTestSegment(sessionID, uint32(i), payload))
			results <- quotaWriteResult{index: i, err: err}
			if err != nil {
				return
			}
		}
	}()
	return results
}

func waitQuotaWrite(t *testing.T, results <-chan quotaWriteResult, want int) {
	t.Helper()
	select {
	case result := <-results:
		if result.err != nil {
			t.Fatalf("write segment %d failed: %v", result.index, result.err)
		}
		if result.index != want {
			t.Fatalf("completed segment = %d, want %d", result.index, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for segment %d to be written", want)
	}
}

// waitUnreadPayload waits until the session owns want unread payload bytes.
// Without a byte quota the accounting is attached after the sender's write
// returns, so the receiver delivery is asynchronous to the writer.
func waitUnreadPayload(t *testing.T, session *Session, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := session.UnreadPayloadBytes(); got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("unread payload bytes = %d, want %d", session.UnreadPayloadBytes(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestReceivePartialReadAccounting proves that unreadBuf keeps the remainder of
// a partially read segment accounted and that only copied bytes are released.
func TestReceivePartialReadAccounting(t *testing.T) {
	const segmentSize = maxPDU
	p := newReceivePair(t)
	session := p.addSession(t, 8)

	payloads := [][]byte{quotaTestPayload(0, segmentSize), quotaTestPayload(1, segmentSize)}
	results := p.writeSegments(8, payloads)
	waitQuotaWrite(t, results, 0)
	waitQuotaWrite(t, results, 1)
	waitUnreadPayload(t, session, int64(2*segmentSize))

	const firstRead = 100
	buf := make([]byte, firstRead)
	if _, err := io.ReadFull(session, buf); err != nil {
		t.Fatalf("ReadFull(firstRead) failed: %v", err)
	}
	wantRemaining := int64(2*segmentSize - firstRead)
	if got := session.UnreadPayloadBytes(); got != wantRemaining {
		t.Fatalf("unread payload bytes after partial read = %d, want %d", got, wantRemaining)
	}
	if got := len(session.unreadBuf); got != segmentSize-firstRead {
		t.Fatalf("unreadBuf length = %d, want %d", got, segmentSize-firstRead)
	}

	// Draining unreadBuf must return exactly the parked remainder.
	restOfFirst := make([]byte, segmentSize-firstRead)
	if _, err := io.ReadFull(session, restOfFirst); err != nil {
		t.Fatalf("ReadFull(restOfFirst) failed: %v", err)
	}
	if got := session.UnreadPayloadBytes(); got != int64(segmentSize) {
		t.Fatalf("unread payload bytes after draining unreadBuf = %d, want %d", got, segmentSize)
	}
	if got := len(session.unreadBuf); got != 0 {
		t.Fatalf("unreadBuf length = %d, want 0", got)
	}
	if !bytes.Equal(append(buf, restOfFirst...), payloads[0]) {
		t.Fatal("partially read segment was reassembled incorrectly")
	}

	second := make([]byte, segmentSize)
	if _, err := io.ReadFull(session, second); err != nil {
		t.Fatalf("ReadFull(second) failed: %v", err)
	}
	if !bytes.Equal(second, payloads[1]) {
		t.Fatal("second segment payload differs")
	}
	if got := session.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("unread payload bytes after full read = %d, want 0", got)
	}
}

// TestReceivePacketDuplicateAndDropAccounting proves that duplicate packets are
// counted once, that the UDP window drop returns the accounting, and that Read
// releases everything exactly once.
func TestReceivePacketDuplicateAndDropAccounting(t *testing.T) {
	session := NewSession(11, false, 1400, nil, nil)
	session.transportProtocol = common.PacketTransport

	first := []byte("second")
	if err := session.input(testDataSegment(11, 1, first, common.PacketTransport)); err != nil {
		t.Fatalf("input(out of order) failed: %v", err)
	}
	if got := session.UnreadPayloadBytes(); got != int64(len(first)) {
		t.Fatalf("unread payload bytes after out-of-order segment = %d, want %d", got, len(first))
	}

	// The retransmitted duplicate replaces the queued copy. The logical
	// payload exists once, so it must be counted once.
	if err := session.input(testDataSegment(11, 1, first, common.PacketTransport)); err != nil {
		t.Fatalf("input(duplicate) failed: %v", err)
	}
	if got := session.UnreadPayloadBytes(); got != int64(len(first)) {
		t.Fatalf("unread payload bytes after duplicate = %d, want %d", got, len(first))
	}
	if got := session.recvBuf.Len(); got != 1 {
		t.Fatalf("recvBuf length after duplicate = %d, want 1", got)
	}

	second := []byte("first")
	if err := session.input(testDataSegment(11, 0, second, common.PacketTransport)); err != nil {
		t.Fatalf("input(first) failed: %v", err)
	}
	if got := session.UnreadPayloadBytes(); got != int64(len(first)+len(second)) {
		t.Fatalf("unread payload bytes after in-order delivery = %d, want %d", got, len(first)+len(second))
	}
	got := make([]byte, len(first)+len(second))
	if _, err := io.ReadFull(session, got); err != nil {
		t.Fatalf("ReadFull() failed: %v", err)
	}
	if !bytes.Equal(got, append(append([]byte(nil), second...), first...)) {
		t.Fatalf("packet payload = %q, want %q", got, append(append([]byte(nil), second...), first...))
	}
	if got := session.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("unread payload bytes after read = %d, want 0", got)
	}

	// Fill the receive window directly, then prove a dropped segment returns
	// its accounting instead of leaking it.
	for i := 0; i < segmentTreeCapacity; i++ {
		if !session.recvBuf.Insert(testDataSegment(11, uint32(i), []byte("x"), common.PacketTransport)) {
			t.Fatalf("failed to prefill recvBuf at %d", i)
		}
	}
	dropped := testDataSegment(11, segmentTreeCapacity+1, []byte("dropped"), common.PacketTransport)
	if err := session.input(dropped); err != nil {
		t.Fatalf("input(dropped) failed: %v", err)
	}
	if got := session.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("unread payload bytes after window drop = %d, want 0", got)
	}
	if dropped.recvOwner != nil {
		t.Fatal("dropped segment is still accounted")
	}
}

// TestLateDeliveryAfterCloseDoesNotRevive proves that a delivery that arrives
// after a session closed returns its accounting instead of reviving the session
// or corrupting a live session.
func TestLateDeliveryAfterCloseDoesNotRevive(t *testing.T) {
	underlay := newBaseUnderlay(true, 1400, nil)
	oldSession := NewSession(31, true, 1400, nil, nil)
	if err := underlay.AddSession(oldSession, nil); err != nil {
		t.Fatalf("AddSession(old) failed: %v", err)
	}
	if err := oldSession.Close(); err != nil {
		t.Fatalf("oldSession.Close() failed: %v", err)
	}

	late := quotaTestSegment(31, 0, []byte("late payload"))
	if underlay.deliverSegmentToSession(oldSession, late) {
		t.Fatal("deliverSegmentToSession() accepted a segment for a closed session")
	}
	if got := oldSession.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("closed session unread payload bytes = %d, want 0", got)
	}

	liveSession := NewSession(32, true, 1400, nil, nil)
	if err := underlay.AddSession(liveSession, nil); err != nil {
		t.Fatalf("AddSession(live) failed: %v", err)
	}
	if got := liveSession.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("live session unread payload bytes = %d, want 0 before delivery", got)
	}
	live := quotaTestSegment(32, 0, []byte("live payload"))
	if !underlay.deliverSegmentToSession(liveSession, live) {
		t.Fatal("deliverSegmentToSession() rejected a segment for a live session")
	}
	if got := liveSession.UnreadPayloadBytes(); got != int64(len("live payload")) {
		t.Fatalf("live session unread payload bytes = %d, want %d", got, len("live payload"))
	}
	if got := oldSession.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("old session accounting was corrupted by a later delivery: %d", got)
	}
	// The base underlay does not start the session input loop, so the segment
	// parks in recvChan; Close must drain it and return the accounting.
	if got := len(liveSession.recvChan); got != 1 {
		t.Fatalf("live session recvChan length = %d, want 1", got)
	}
	if err := liveSession.Close(); err != nil {
		t.Fatalf("liveSession.Close() failed: %v", err)
	}
	if got := liveSession.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("live session unread payload bytes after close = %d, want 0", got)
	}
}

// TestRemoveSessionDrainsReceivePayload proves that removing a session drains
// and releases payload parked in the delivery channel.
func TestRemoveSessionDrainsReceivePayload(t *testing.T) {
	underlay := newBaseUnderlay(true, 1400, nil)
	session := NewSession(41, true, 1400, nil, nil)
	if err := underlay.AddSession(session, nil); err != nil {
		t.Fatalf("AddSession() failed: %v", err)
	}
	session.forwardStateTo(sessionAttached)
	parked := quotaTestSegment(41, 0, []byte("parked"))
	underlay.deliverSegmentToSession(session, parked)
	if got := session.UnreadPayloadBytes(); got != int64(len("parked")) {
		t.Fatalf("unread payload bytes = %d, want %d", got, len("parked"))
	}
	if err := underlay.RemoveSession(session); err != nil {
		t.Fatalf("RemoveSession() failed: %v", err)
	}
	if got := session.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("unread payload bytes after RemoveSession = %d, want 0", got)
	}
	if got := len(session.recvChan); got != 0 {
		t.Fatalf("recvChan length after RemoveSession = %d, want 0", got)
	}
}

// TestSetReceiveByteQuotaWakesWaiter proves that a runtime quota update wakes a
// parked admission and that the observable bound remains exact.
func TestSetReceiveByteQuotaWakesWaiter(t *testing.T) {
	session := NewSession(62, true, 1400, nil, nil)
	session.setReceiveByteQuota(MinReceiveByteQuota)
	if !session.tryReserveReceivePayload(MinReceiveByteQuota) {
		t.Fatal("failed to reserve the full quota")
	}
	result := make(chan receiveAdmission, 1)
	go func() {
		result <- session.waitReceivePayload(context.Background(), make(chan struct{}), 1)
	}()
	select {
	case admission := <-result:
		t.Fatalf("admission returned %v before the quota changed", admission)
	case <-time.After(100 * time.Millisecond):
	}
	session.setReceiveByteQuota(MinReceiveByteQuota + 1)
	select {
	case admission := <-result:
		if admission != receiveAdmitted {
			t.Fatalf("admission = %v, want receiveAdmitted", admission)
		}
	case <-time.After(time.Second):
		t.Fatal("quota update did not wake the parked admission")
	}
	if got := session.UnreadPayloadBytes(); got != MinReceiveByteQuota+1 {
		t.Fatalf("unread payload bytes = %d, want %d", got, MinReceiveByteQuota+1)
	}
}

// TestReceiveLowEntropyPayloadAccounting proves that the accounting counts
// decoded application payload bytes, not the larger low-entropy wire encoding.
func TestReceiveLowEntropyPayloadAccounting(t *testing.T) {
	p := newReceivePair(t)
	session := p.addSession(t, 61)

	payload := quotaTestPayload(0, 1000)
	payloadLen, err := lowEntropyEncodedPayloadLen(len(payload), appctlpb.LowEntropyMode_LOW_ENTROPY_MODE_32)
	if err != nil {
		t.Fatalf("lowEntropyEncodedPayloadLen() failed: %v", err)
	}
	if payloadLen <= uint16(len(payload)) {
		t.Fatalf("test payload is not expanded: wire %d, plaintext %d", payloadLen, len(payload))
	}
	seg := &segment{
		metadata: &dataAckStruct{
			baseStruct:             baseStruct{protocol: uint8(dataServerToClientLowEntropy)},
			lowEntropyMode:         uint8(appctlpb.LowEntropyMode_LOW_ENTROPY_MODE_32),
			sessionID:              61,
			seq:                    0,
			windowSize:             uint16(segmentTreeCapacity),
			payloadLen:             payloadLen,
			extractedPayloadLen:    uint16(len(payload)),
			lowEntropyMaskRotation: uint8(appctlpb.LowEntropyMaskRotation_LOW_ENTROPY_MASK_NO_ROTATION),
		},
		payload:   payload,
		transport: common.StreamTransport,
	}
	written := make(chan error, 1)
	go func() {
		written <- p.sender.writeOneSegment(seg)
	}()
	select {
	case err := <-written:
		if err != nil {
			t.Fatalf("writeOneSegment() failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out writing the low entropy segment")
	}
	// The decoded payload must be what is counted, not the 2x wire encoding.
	waitUnreadPayload(t, session, int64(len(payload)))
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(session, got); err != nil {
		t.Fatalf("ReadFull() failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("decoded low entropy payload differs")
	}
	if got := session.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("unread payload bytes after read = %d, want 0", got)
	}
}
