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
	"context"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enfein/mieru/v3/pkg/cipher"
)

// newTCPQuotaPair creates a real loopback TCP stream underlay pair so the
// measurement includes kernel socket buffering and real TCP backpressure.
func newTCPQuotaPair(tb testing.TB, quota int64) (sender, receiver *StreamUnderlay, senderConn, receiverConn net.Conn, session *Session, cleanup func()) {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("net.Listen() failed: %v", err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()
	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		tb.Fatalf("net.Dial() failed: %v", err)
	}
	serverConn, ok := <-accepted
	if !ok {
		tb.Fatalf("listener.Accept() failed")
	}
	_ = listener.Close()

	block, err := cipher.BlockCipherFromPassword([]byte("receive-quota-bench"), false)
	if err != nil {
		tb.Fatalf("BlockCipherFromPassword() failed: %v", err)
	}
	block.SetBlockContext(cipher.BlockContext{UserName: "receive-quota-bench"})
	sender = &StreamUnderlay{
		baseUnderlay:       *newBaseUnderlay(true, 1400, nil),
		conn:               clientConn,
		block:              block,
		sessionCleanTicker: time.NewTicker(time.Hour),
	}
	receiver = &StreamUnderlay{
		baseUnderlay:       *newBaseUnderlay(true, 1400, nil),
		conn:               serverConn,
		block:              block.Clone(),
		sessionCleanTicker: time.NewTicker(time.Hour),
	}
	receiver.exclusive = true
	receiver.recvQuota.Store(quota)
	session = NewSession(71, true, 1400, nil, nil)
	if err := receiver.AddSession(session, nil); err != nil {
		tb.Fatalf("AddSession() failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		_ = receiver.RunEventLoop(ctx)
		close(loopDone)
	}()
	cleanup = func() {
		cancel()
		_ = receiver.Close()
		_ = sender.Close()
		_ = clientConn.Close()
		_ = serverConn.Close()
		select {
		case <-loopDone:
		case <-time.After(2 * time.Second):
		}
	}
	return sender, receiver, clientConn, serverConn, session, cleanup
}

// BenchmarkStreamReceiveQuota measures exclusive stream throughput under
// different receive byte quotas. Run with -benchtime to control the sample.
func BenchmarkStreamReceiveQuota(b *testing.B) {
	const total = 32 << 20
	payload := quotaTestPayload(0, maxPDU)
	quotas := []struct {
		name  string
		quota int64
	}{
		{"unlimited", 0},
		{"64KiB", MinReceiveByteQuota},
		{"256KiB", DefaultReceiveByteQuota},
		{"1MiB", 1 << 20},
	}
	for _, q := range quotas {
		b.Run(q.name, func(b *testing.B) {
			b.SetBytes(total)
			for i := 0; i < b.N; i++ {
				sender, _, _, _, session, cleanup := newTCPQuotaPair(b, q.quota)
				readDone := make(chan error, 1)
				go func() {
					_, err := io.CopyN(io.Discard, session, total)
					readDone <- err
				}()
				writeDone := make(chan error, 1)
				go func() {
					for sent := 0; sent < total; sent += len(payload) {
						seg := quotaTestSegment(71, uint32(sent/len(payload)), payload)
						if err := sender.writeOneSegment(seg); err != nil {
							writeDone <- err
							return
						}
					}
					writeDone <- nil
				}()
				if err := <-writeDone; err != nil {
					cleanup()
					b.Fatalf("writeOneSegment() failed: %v", err)
				}
				if err := <-readDone; err != nil {
					cleanup()
					b.Fatalf("read failed: %v", err)
				}
				cleanup()
			}
		})
	}
}

// TestReceiveQuotaResidencyMeasurement logs the achieved unread payload bound
// and the live heap for a writer that keeps sending while the application does
// not read. It is a measurement helper, not a timing assertion.
func TestReceiveQuotaResidencyMeasurement(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement is slow in -short mode")
	}
	const total = 16 << 20
	const segmentSize = maxPDU
	payload := quotaTestPayload(0, segmentSize)
	for _, quota := range []int64{MinReceiveByteQuota, DefaultReceiveByteQuota, 0} {
		p := newReceiveQuotaPair(t, quota, true)
		session := p.addSession(t, 81)
		var written atomic.Int64
		writeDone := make(chan struct{})
		go func() {
			defer close(writeDone)
			for sent := 0; sent < total; sent += segmentSize {
				err := p.sender.writeOneSegment(quotaTestSegment(81, uint32(sent/segmentSize), payload))
				if err != nil {
					return
				}
				written.Add(int64(segmentSize))
			}
		}()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if quota > 0 && session.UnreadPayloadBytes() >= quota {
				break
			}
			if quota == 0 && written.Load() >= total {
				break
			}
			time.Sleep(time.Millisecond)
		}
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		t.Logf("quota=%d written=%d unread=%d heapAlloc=%d goroutines=%d",
			quota, written.Load(), session.UnreadPayloadBytes(), stats.HeapAlloc, runtime.NumGoroutine())
		if quota > 0 && session.UnreadPayloadBytes() > quota {
			t.Errorf("quota=%d observed unread=%d above the declared bound", quota, session.UnreadPayloadBytes())
		}
		_ = session.Close()
		p.close()
		<-writeDone
	}
}
