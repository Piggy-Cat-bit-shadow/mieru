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
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/enfein/mieru/v3/pkg/appctl/appctlpb"
	"github.com/enfein/mieru/v3/pkg/cipher"
	"github.com/enfein/mieru/v3/pkg/common"
)

// newWriteEndTestSession returns an established session whose peer capability
// state is already settled, so the state machine can be driven without an
// underlay. peerCapable controls what the peer advertised.
func newWriteEndTestSession(t *testing.T, isClient bool, transport common.TransportProtocol, peerCapable bool) *Session {
	t.Helper()
	s := NewSession(7, isClient, 1400, nil, nil)
	s.transportProtocol = transport
	s.forwardStateTo(sessionEstablished)
	// The unit tests drive the state machine directly; the open session
	// request is a separate concern and must not be queued here.
	s.openSessionRequestSent.Store(true)
	if peerCapable {
		s.notePeerSessionFlags(sessionFlagWriteEndCapable)
	} else {
		s.notePeerSessionFlags(0)
	}
	return s
}

// writeEndTestDataSegment builds a data segment in the direction that a peer
// of s would send.
func writeEndTestDataSegment(s *Session, seq uint32, payload []byte, flags uint8) *segment {
	protocol := dataClientToServer
	if s.isClient {
		protocol = dataServerToClient
	}
	return &segment{
		metadata: &dataAckStruct{
			baseStruct: baseStruct{
				protocol: uint8(protocol),
				flags:    flags,
			},
			sessionID:  s.id,
			seq:        seq,
			windowSize: uint16(segmentTreeCapacity),
			payloadLen: uint16(len(payload)),
		},
		payload:   append([]byte(nil), payload...),
		transport: s.transportProtocol,
	}
}

func TestMetadataWriteEndFlags(t *testing.T) {
	// A sessionStruct carries the capability flag in the reserved byte.
	ss := &sessionStruct{
		baseStruct: baseStruct{
			protocol: uint8(openSessionRequest),
			flags:    sessionFlagWriteEndCapable,
		},
		sessionID: 12,
		seq:       3,
	}
	wire := ss.Marshal()
	if wire[1] != sessionFlagWriteEndCapable {
		t.Fatalf("session metadata byte 1 = %#x, want %#x", wire[1], sessionFlagWriteEndCapable)
	}
	var decoded sessionStruct
	if err := decoded.Unmarshal(wire); err != nil {
		t.Fatalf("sessionStruct.Unmarshal() failed: %v", err)
	}
	if decoded.flags != sessionFlagWriteEndCapable {
		t.Fatalf("decoded flags = %#x, want %#x", decoded.flags, sessionFlagWriteEndCapable)
	}

	// A plain data frame carries the write-end flag in the same byte.
	das := &dataAckStruct{
		baseStruct: baseStruct{
			protocol: uint8(dataClientToServer),
			flags:    dataFlagWriteEnd,
		},
		sessionID: 12,
		seq:       4,
	}
	wire = das.Marshal()
	if wire[1] != dataFlagWriteEnd {
		t.Fatalf("data metadata byte 1 = %#x, want %#x", wire[1], dataFlagWriteEnd)
	}
	var decodedData dataAckStruct
	if err := decodedData.Unmarshal(wire); err != nil {
		t.Fatalf("dataAckStruct.Unmarshal() failed: %v", err)
	}
	if decodedData.flags != dataFlagWriteEnd {
		t.Fatalf("decoded data flags = %#x, want %#x", decodedData.flags, dataFlagWriteEnd)
	}

	// A low entropy data frame uses byte 1 as the mode. The write-end flag
	// must never leak into it: a write end is always a plain data frame.
	lowEntropy := &dataAckStruct{
		baseStruct: baseStruct{protocol: uint8(dataClientToServerLowEntropy)},
		// LOW_ENTROPY_MODE_32 with a mask that has 16 one-bits is a valid
		// parameter set for an empty frame.
		lowEntropyMode:         uint8(appctlpb.LowEntropyMode_LOW_ENTROPY_MODE_32),
		sessionID:              12,
		seq:                    5,
		lowEntropyMask:         0x0f0f0f0f,
		lowEntropyMaskRotation: uint8(appctlpb.LowEntropyMaskRotation_LOW_ENTROPY_MASK_ROTATE_LEFT_3),
	}
	wire = lowEntropy.Marshal()
	if wire[1] != lowEntropy.lowEntropyMode {
		t.Fatalf("low entropy metadata byte 1 = %#x, want mode %#x", wire[1], lowEntropy.lowEntropyMode)
	}
	var decodedLowEntropy dataAckStruct
	if err := decodedLowEntropy.Unmarshal(wire); err != nil {
		t.Fatalf("low entropy dataAckStruct.Unmarshal() failed: %v", err)
	}
	if decodedLowEntropy.flags != 0 {
		t.Fatalf("low entropy frame decoded flags = %#x, want 0", decodedLowEntropy.flags)
	}
}

// TestReadWriteEndDeliversPayloadThenEOF is the core receive contract: payload
// queued before the write end is returned first (including out of order packet
// delivery), and Read then reports io.EOF without the session being closed.
func TestReadWriteEndDeliversPayloadThenEOF(t *testing.T) {
	transports := []struct {
		name      string
		transport common.TransportProtocol
	}{
		{"stream", common.StreamTransport},
		{"packet", common.PacketTransport},
	}
	for _, tc := range transports {
		t.Run(tc.name, func(t *testing.T) {
			transport := tc.transport
			s := newWriteEndTestSession(t, true, transport, false)

			// Deliver "world" before "hello" on the packet transport to prove
			// the write end is ordered against payload, not against arrival.
			segments := []*segment{
				writeEndTestDataSegment(s, 1, []byte("world"), 0),
				writeEndTestDataSegment(s, 0, []byte("hello "), 0),
			}
			if transport == common.StreamTransport {
				segments[0], segments[1] = segments[1], segments[0]
			}
			for _, seg := range segments {
				if err := s.inputData(seg); err != nil {
					t.Fatalf("inputData(payload) failed: %v", err)
				}
			}
			writeEnd := writeEndTestDataSegment(s, 2, nil, dataFlagWriteEnd)
			if err := s.inputData(writeEnd); err != nil {
				t.Fatalf("inputData(write end) failed: %v", err)
			}
			if s.readWriteEnd.Load() {
				t.Fatal("read direction ended before Read consumed the write end")
			}

			buf := make([]byte, 64)
			n, err := s.Read(buf)
			if n != len("hello world") || !bytes.Equal(buf[:n], []byte("hello world")) {
				t.Fatalf("Read() = %d %q, want %d %q", n, buf[:n], len("hello world"), "hello world")
			}
			if err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("Read() error = %v, want nil or io.EOF", err)
			}

			n, err = s.Read(buf)
			if n != 0 || !errors.Is(err, io.EOF) {
				t.Fatalf("Read() after write end = %d, %v; want 0, io.EOF", n, err)
			}
			// Repeated reads stay at EOF instead of blocking.
			if n, err = s.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
				t.Fatalf("repeated Read() after write end = %d, %v; want 0, io.EOF", n, err)
			}
			// Every received byte is accounted and has been returned.
			if got := s.UnreadPayloadBytes(); got != 0 {
				t.Fatalf("UnreadPayloadBytes() = %d, want 0", got)
			}
			// The session itself is still open: only the read direction ended.
			if s.closeRequested.Load() || s.isState(sessionClosed) {
				t.Fatal("write end closed the whole session")
			}
		})
	}
}

// TestReadWriteEndDropsPayloadAfterEOF proves payload that arrives after the
// write end (a protocol violation, or a late/duplicate packet) cannot hold
// receive quota that the application will never consume.
func TestReadWriteEndDropsPayloadAfterEOF(t *testing.T) {
	transports := []struct {
		name      string
		transport common.TransportProtocol
	}{
		{"stream", common.StreamTransport},
		{"packet", common.PacketTransport},
	}
	for _, tc := range transports {
		t.Run(tc.name, func(t *testing.T) {
			transport := tc.transport
			s := newWriteEndTestSession(t, false, transport, false)
			if err := s.inputData(writeEndTestDataSegment(s, 0, []byte("request"), 0)); err != nil {
				t.Fatalf("inputData(payload) failed: %v", err)
			}
			if err := s.inputData(writeEndTestDataSegment(s, 1, nil, dataFlagWriteEnd)); err != nil {
				t.Fatalf("inputData(write end) failed: %v", err)
			}
			buf := make([]byte, 32)
			n, err := s.Read(buf)
			if n != len("request") || !bytes.Equal(buf[:n], []byte("request")) {
				t.Fatalf("Read() = %d %q, want request", n, buf[:n])
			}
			if err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("Read() error = %v, want nil or io.EOF", err)
			}
			if _, err := s.Read(buf); !errors.Is(err, io.EOF) {
				t.Fatalf("Read() = %v, want io.EOF", err)
			}

			// Late payload, and a duplicate of a pre-write-end sequence, are
			// both dropped and both release their accounting.
			if err := s.inputData(writeEndTestDataSegment(s, 2, []byte("late"), 0)); err != nil {
				t.Fatalf("inputData(late) failed: %v", err)
			}
			if err := s.inputData(writeEndTestDataSegment(s, 1, nil, dataFlagWriteEnd)); err != nil {
				t.Fatalf("inputData(duplicate write end) failed: %v", err)
			}
			if got := s.UnreadPayloadBytes(); got != 0 {
				t.Fatalf("UnreadPayloadBytes() after late payload = %d, want 0", got)
			}
			if _, err := s.Read(buf); !errors.Is(err, io.EOF) {
				t.Fatalf("Read() after late payload = %v, want io.EOF", err)
			}
		})
	}
}

// TestWriteEndFrameWithPayloadRejected proves a malformed write end is a
// protocol error rather than payload silently delivered after EOF.
func TestWriteEndFrameWithPayloadRejected(t *testing.T) {
	s := newWriteEndTestSession(t, true, common.StreamTransport, false)
	malformed := writeEndTestDataSegment(s, 0, []byte("data"), dataFlagWriteEnd)
	if err := s.inputData(malformed); err == nil {
		t.Fatal("inputData() accepted a write end frame with payload")
	}
	if got := s.UnreadPayloadBytes(); got != 0 {
		t.Fatalf("UnreadPayloadBytes() = %d, want 0 after a rejected frame", got)
	}
}

// TestCloseWriteUnsupportedWithOldPeer is the compatibility contract: an old
// peer advertises nothing, so CloseWrite sends nothing and says so. The caller
// can fall back to Close, which is exactly the old behaviour.
func TestCloseWriteUnsupportedWithOldPeer(t *testing.T) {
	s := newWriteEndTestSession(t, true, common.StreamTransport, false)
	if err := s.CloseWrite(); !errors.Is(err, ErrWriteEndUnsupported) {
		t.Fatalf("CloseWrite() error = %v, want ErrWriteEndUnsupported", err)
	}
	if s.writeEndSent {
		t.Fatal("CloseWrite() marked the write direction as ended for an old peer")
	}
	if got := s.sendQueue.Len(); got != 0 {
		t.Fatalf("send queue holds %d frame(s), want 0: nothing may be sent to an old peer", got)
	}
	if s.closeRequested.Load() {
		t.Fatal("CloseWrite() closed the session")
	}
	// Repeated calls return the same verdict, and a later full close is still
	// available to the caller.
	if err := s.CloseWrite(); !errors.Is(err, ErrWriteEndUnsupported) {
		t.Fatalf("second CloseWrite() error = %v, want ErrWriteEndUnsupported", err)
	}
}

// TestCloseWriteOrdersWriteEndAfterPayload covers the sending state machine:
// the write end follows every queued payload segment, is idempotent, and
// rejects a later Write with io.ErrClosedPipe.
func TestCloseWriteOrdersWriteEndAfterPayload(t *testing.T) {
	s := newWriteEndTestSession(t, true, common.StreamTransport, true)
	payload := &segment{
		metadata: &dataAckStruct{
			baseStruct: baseStruct{protocol: uint8(dataClientToServer)},
			sessionID:  s.id,
			seq:        0,
			payloadLen: uint16(len("request")),
		},
		payload:   []byte("request"),
		transport: common.StreamTransport,
	}
	if !s.sendQueue.Insert(payload) {
		t.Fatal("insert payload to send queue failed")
	}
	s.nextSend.Store(1)

	if err := s.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite() failed: %v", err)
	}
	if !s.writeEndSent {
		t.Fatal("CloseWrite() did not mark the write direction as ended")
	}

	var queued []*segment
	s.sendQueue.Ascend(func(iter *segment) bool {
		queued = append(queued, iter)
		return true
	})
	if len(queued) != 2 {
		t.Fatalf("send queue holds %d frames, want 2", len(queued))
	}
	if !bytes.Equal(queued[0].payload, []byte("request")) {
		t.Fatalf("first queued frame payload = %q, want request", queued[0].payload)
	}
	if !isWriteEndSegment(queued[1]) {
		t.Fatalf("second queued frame = %v, want a write end", queued[1])
	}
	if len(queued[1].payload) != 0 {
		t.Fatalf("write end frame payload = %q, want empty", queued[1].payload)
	}
	seq, err := queued[1].Seq()
	if err != nil || seq != 1 {
		t.Fatalf("write end sequence = %d, %v; want 1, nil", seq, err)
	}

	// Write after CloseWrite fails with the clear closed-pipe error.
	if _, err := s.Write([]byte("more")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Write() after CloseWrite() error = %v, want io.ErrClosedPipe", err)
	}
	// CloseWrite is idempotent and does not queue a second write end.
	if err := s.CloseWrite(); err != nil {
		t.Fatalf("second CloseWrite() failed: %v", err)
	}
	if got := s.sendQueue.Len(); got != 2 {
		t.Fatalf("send queue holds %d frames after repeated CloseWrite, want 2", got)
	}
}

// TestCloseWriteUnblockedByClose proves Close cancels a pending capability
// wait promptly instead of leaving the write side parked for the whole
// handshake timeout. The peer here is a real TCP socket that never answers.
func TestCloseWriteUnblockedByClose(t *testing.T) {
	s := newSilentStreamSession(t)

	result := make(chan error, 1)
	go func() {
		result <- s.CloseWrite()
	}()
	select {
	case err := <-result:
		t.Fatalf("CloseWrite() returned %v before the peer answered", err)
	case <-time.After(100 * time.Millisecond):
	}

	closed := make(chan error, 1)
	go func() {
		closed <- s.Close()
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close() failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not return")
	}
	select {
	case err := <-result:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("CloseWrite() error = %v, want io.ErrClosedPipe", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CloseWrite() did not unblock after Close()")
	}
}

// newSilentStreamSession creates a real client stream session whose peer
// accepts the connection and then never writes a byte.
func newSilentStreamSession(t *testing.T) *Session {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() failed: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial() failed: %v", err)
	}
	select {
	case peer := <-accepted:
		t.Cleanup(func() { peer.Close() })
	case <-time.After(3 * time.Second):
		t.Fatal("listener did not accept the connection")
	}

	block, err := cipher.BlockCipherFromPassword([]byte("write-end-test"), true)
	if err != nil {
		t.Fatalf("BlockCipherFromPassword() failed: %v", err)
	}
	underlay := &StreamUnderlay{
		baseUnderlay:       *newBaseUnderlay(true, 1400, nil),
		conn:               conn,
		block:              block,
		sessionCleanTicker: time.NewTicker(time.Hour),
	}
	s := NewSession(11, true, 1400, nil, nil)
	if err := underlay.AddSession(s, nil); err != nil {
		t.Fatalf("AddSession() failed: %v", err)
	}
	t.Cleanup(func() {
		s.Close()
		underlay.Close()
	})
	return s
}
