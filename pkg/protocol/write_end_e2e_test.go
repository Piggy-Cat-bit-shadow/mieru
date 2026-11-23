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
	"testing"
	"time"

	"github.com/enfein/mieru/v3/pkg/cipher"
	"github.com/enfein/mieru/v3/pkg/common"
)

// writeEndServer is the destination side of the end-to-end test. It reads the
// request until EOF and only then writes the response, so a response that
// reaches the client proves the client's write direction ended at the server
// application without the session being closed.
type writeEndServerResult struct {
	acceptErr     error
	request       []byte
	readErr       error
	writeErr      error
	closeWriteErr error
}

func newWriteEndE2ETest(t *testing.T, transport common.TransportProtocol, multiplexFactor int) (*Mux, *Mux) {
	t.Helper()
	var serverProperties, clientProperties UnderlayProperties
	if transport == common.StreamTransport {
		port, err := common.UnusedTCPPort()
		if err != nil {
			t.Fatalf("common.UnusedTCPPort() failed: %v", err)
		}
		serverProperties = NewUnderlayProperties(1400, transport, &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: port}, nil)
		clientProperties = NewUnderlayProperties(1400, transport, nil, &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: port})
	} else {
		port, err := common.UnusedUDPPort()
		if err != nil {
			t.Fatalf("common.UnusedUDPPort() failed: %v", err)
		}
		serverProperties = NewUnderlayProperties(1400, transport, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: port}, nil)
		clientProperties = NewUnderlayProperties(1400, transport, nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: port})
	}

	serverMux := NewMux(false).
		SetServerUsers(users).
		SetEndpoints([]UnderlayProperties{serverProperties})
	if err := serverMux.Start(); err != nil {
		t.Fatalf("server mux Start() failed: %v", err)
	}
	// The listener goroutine is up when Start returns; a short delay keeps the
	// first dial away from the window where the OS backlog is not established.
	time.Sleep(100 * time.Millisecond)

	clientMux := NewMux(true).
		SetClientUserNamePassword("xiaochitang", cipher.HashPassword([]byte("kuiranbudong"), []byte("xiaochitang"))).
		SetClientMultiplexFactor(multiplexFactor).
		SetEndpoints([]UnderlayProperties{clientProperties})
	t.Cleanup(func() {
		if err := clientMux.Close(); err != nil {
			t.Errorf("client mux Close() failed: %v", err)
		}
		if err := serverMux.Close(); err != nil {
			t.Errorf("server mux Close() failed: %v", err)
		}
	})
	return serverMux, clientMux
}

// runWriteEndDestination serves exactly one session: it reads until the client
// ends its write direction, then writes the response and ends its own write
// direction. It keeps the full close waiting for the test so the response
// cannot be discarded by an early close.
func runWriteEndDestination(t *testing.T, serverMux *Mux, response []byte, clientClosed <-chan struct{}) <-chan writeEndServerResult {
	t.Helper()
	resultCh := make(chan writeEndServerResult, 1)
	go func() {
		var result writeEndServerResult
		conn, err := serverMux.Accept()
		if err != nil {
			result.acceptErr = err
			resultCh <- result
			return
		}
		// Read returns io.EOF only after the client's CloseWrite reached this
		// session, because the session itself is still open. The deadline
		// bounds the control case, where the peer disappeared instead of
		// ending its write direction.
		if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			result.acceptErr = err
			_ = conn.Close()
			resultCh <- result
			return
		}
		result.request, result.readErr = io.ReadAll(conn)
		_ = conn.SetReadDeadline(time.Time{})
		if result.readErr == nil {
			_, result.writeErr = conn.Write(response)
			if writeCloser, ok := conn.(interface{ CloseWrite() error }); ok {
				result.closeWriteErr = writeCloser.CloseWrite()
			} else {
				result.closeWriteErr = ErrWriteEndUnsupported
			}
		}
		select {
		case <-clientClosed:
		case <-time.After(15 * time.Second):
		}
		_ = conn.Close()
		resultCh <- result
	}()
	return resultCh
}

func readAllWithDeadline(t *testing.T, conn net.Conn) ([]byte, error) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() failed: %v", err)
	}
	defer conn.SetReadDeadline(time.Time{})
	return io.ReadAll(conn)
}

// TestWriteEndEndToEnd is the real oracle: a real client Session and a real
// server Session over a real TCP or UDP underlay. The client writes a request,
// CloseWrite()s, the server reads EOF, writes a full response, and the client
// reads the complete response - then both close cleanly.
func TestWriteEndEndToEnd(t *testing.T) {
	transports := []struct {
		name      string
		transport common.TransportProtocol
	}{
		{"tcp", common.StreamTransport},
		{"udp", common.PacketTransport},
	}
	multiplexing := []struct {
		name   string
		factor int
	}{
		{"exclusive", 0},
		{"shared", 2},
	}
	for _, tc := range transports {
		for _, mc := range multiplexing {
			t.Run(tc.name+"/"+mc.name, func(t *testing.T) {
				request := bytes.Repeat([]byte("request-payload-"), 64)    // 1024 bytes
				response := bytes.Repeat([]byte("response-payload-"), 256) // 4352 bytes

				serverMux, clientMux := newWriteEndE2ETest(t, tc.transport, mc.factor)
				clientClosed := make(chan struct{})
				resultCh := runWriteEndDestination(t, serverMux, response, clientClosed)

				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				clientConn, err := clientMux.DialContext(ctx)
				if err != nil {
					t.Fatalf("DialContext() failed: %v", err)
				}
				defer clientConn.Close()

				if _, ok := clientConn.(*Session); !ok {
					t.Fatalf("DialContext() returned %T, want *Session", clientConn)
				}
				writeCloser, ok := clientConn.(interface{ CloseWrite() error })
				if !ok {
					t.Fatal("the client session does not implement CloseWrite")
				}

				if _, err := clientConn.Write(request); err != nil {
					t.Fatalf("Write(request) failed: %v", err)
				}
				if err := writeCloser.CloseWrite(); err != nil {
					t.Fatalf("CloseWrite() failed: %v", err)
				}

				got, err := readAllWithDeadline(t, clientConn)
				if err != nil {
					t.Fatalf("reading the post-EOF response failed: %v (received %d of %d bytes)", err, len(got), len(response))
				}
				if !bytes.Equal(got, response) {
					t.Fatalf("response mismatch: got %d bytes, want %d", len(got), len(response))
				}
				if err := clientConn.Close(); err != nil {
					t.Fatalf("client Close() failed: %v", err)
				}
				close(clientClosed)

				result := <-resultCh
				if result.acceptErr != nil {
					t.Fatalf("server Accept() failed: %v", result.acceptErr)
				}
				if result.readErr != nil {
					t.Fatalf("server did not read the request to EOF: %v", result.readErr)
				}
				if !bytes.Equal(result.request, request) {
					t.Fatalf("server request mismatch: got %d bytes, want %d", len(result.request), len(request))
				}
				if result.writeErr != nil {
					t.Fatalf("server Write(response) failed: %v", result.writeErr)
				}
				if result.closeWriteErr != nil {
					t.Fatalf("server CloseWrite() failed: %v", result.closeWriteErr)
				}
			})
		}
	}
}

// TestFullCloseBeforeResponseDoesNotDeliverResponse is the control: with a full
// Close instead of CloseWrite, the destination's response must not reach the
// client. It proves the end-to-end test above can fail, and that the result is
// produced by the write end rather than by an early close.
func TestFullCloseBeforeResponseDoesNotDeliverResponse(t *testing.T) {
	transports := []struct {
		name      string
		transport common.TransportProtocol
	}{
		{"tcp", common.StreamTransport},
		{"udp", common.PacketTransport},
	}
	for _, tc := range transports {
		t.Run(tc.name, func(t *testing.T) {
			request := bytes.Repeat([]byte("request-payload-"), 64)
			response := bytes.Repeat([]byte("response-payload-"), 256)

			serverMux, clientMux := newWriteEndE2ETest(t, tc.transport, 0)
			clientClosed := make(chan struct{})
			resultCh := runWriteEndDestination(t, serverMux, response, clientClosed)

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			clientConn, err := clientMux.DialContext(ctx)
			if err != nil {
				t.Fatalf("DialContext() failed: %v", err)
			}
			defer clientConn.Close()
			if _, err := clientConn.Write(request); err != nil {
				t.Fatalf("Write(request) failed: %v", err)
			}
			// Full close: the old behaviour when the connection does not offer
			// CloseWrite.
			if err := clientConn.Close(); err != nil {
				t.Fatalf("client Close() failed: %v", err)
			}

			got, readErr := readAllWithDeadline(t, clientConn)
			if len(got) != 0 {
				t.Fatalf("a full close delivered %d response bytes, want 0", len(got))
			}
			if readErr != nil && readErr != io.EOF {
				t.Fatalf("Read() after a full close error = %v, want io.EOF", readErr)
			}
			close(clientClosed)

			result := <-resultCh
			if result.acceptErr != nil {
				t.Fatalf("server Accept() failed: %v", result.acceptErr)
			}
			// The server may see EOF (a full close also ends Read) or a read
			// timeout (a full close is not a reliable packet). Either way it
			// must not be able to deliver the response. If it did see a clean
			// EOF, the write that follows must fail because the session is
			// already closed.
			if result.readErr == nil {
				if result.writeErr == nil {
					t.Fatal("server Write(response) succeeded after the client fully closed the session")
				}
				t.Logf("destination read EOF, then failed to write as required: %v", result.writeErr)
			} else {
				t.Logf("destination did not observe a clean EOF after a full close (%v); the response was still not delivered", result.readErr)
			}
		})
	}
}
