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

package client

import (
	"errors"
	"strings"
	"testing"

	pb "github.com/enfein/mieru/v3/pkg/appctl/appctlpb"
	"github.com/enfein/mieru/v3/pkg/protocol"
	"google.golang.org/protobuf/proto"
)

func TestStoreRejectProfileDialer(t *testing.T) {
	c := NewClient()
	profile := generateClientProfile(t)
	profile.Dialer = &pb.ClientDialer{
		Protocol:           pb.ProxyProtocol_SOCKS5_PROXY_PROTOCOL.Enum(),
		Host:               proto.String("127.0.0.1"),
		Port:               proto.Int32(1081),
		Socks5UDPAssociate: proto.Bool(true),
	}

	err := c.Store(&ClientConfig{Profile: profile})
	if err == nil {
		t.Fatal("Store() error = nil, want error")
	}
	if !errors.Is(err, ErrInvalidClientConfig) {
		t.Fatalf("Store() error = %v, want ErrInvalidClientConfig", err)
	}
	if !strings.Contains(err.Error(), "client profile dialer is not supported by client API") {
		t.Fatalf("Store() error = %q, want profile dialer rejection", err.Error())
	}
}

func TestStoreReceiveByteQuota(t *testing.T) {
	muxOff := &pb.MultiplexingConfig{Level: pb.MultiplexingLevel_MULTIPLEXING_OFF.Enum()}
	muxLow := &pb.MultiplexingConfig{Level: pb.MultiplexingLevel_MULTIPLEXING_LOW.Enum()}
	tests := []struct {
		name         string
		quota        int64
		multiplexing *pb.MultiplexingConfig
		wantErr      string
	}{
		{name: "disabled", quota: 0, multiplexing: muxLow},
		{name: "negative", quota: -1, multiplexing: muxOff, wantErr: "receive byte quota -1 is invalid"},
		{name: "below minimum", quota: protocol.MinReceiveByteQuota - 1, multiplexing: muxOff, wantErr: "is invalid"},
		{name: "minimum", quota: protocol.MinReceiveByteQuota, multiplexing: muxOff},
		{name: "shared underlay", quota: 4 * protocol.MinReceiveByteQuota, multiplexing: muxLow, wantErr: "requires multiplexing off"},
		{name: "default multiplexing", quota: 4 * protocol.MinReceiveByteQuota, wantErr: "requires multiplexing off"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := generateClientProfile(t)
			profile.Multiplexing = test.multiplexing
			err := NewClient().Store(&ClientConfig{Profile: profile, ReceiveByteQuota: test.quota})
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("Store() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Store() error = nil, want error containing %q", test.wantErr)
			}
			if !errors.Is(err, ErrInvalidClientConfig) {
				t.Fatalf("Store() error = %v, want ErrInvalidClientConfig", err)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Store() error = %q, want error containing %q", err.Error(), test.wantErr)
			}
		})
	}
}

func generateClientProfile(t *testing.T) *pb.ClientProfile {
	t.Helper()
	return &pb.ClientProfile{
		ProfileName: proto.String("default"),
		User: &pb.User{
			Name:     proto.String("user"),
			Password: proto.String("password"),
		},
		Servers: []*pb.ServerEndpoint{
			{
				IpAddress: proto.String("127.0.0.1"),
				PortBindings: []*pb.PortBinding{
					{
						Port:     proto.Int32(10001),
						Protocol: pb.TransportProtocol_TCP.Enum(),
					},
				},
			},
		},
	}
}
