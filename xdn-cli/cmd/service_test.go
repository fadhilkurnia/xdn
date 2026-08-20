package cmd

import "testing"

func TestParseNodeSocketAddress(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		wantHost string
		wantIP   string
		wantPort int
		wantOK   bool
	}{
		{
			name:     "ipv4 with hostname",
			in:       "c240g5.wisc.cloudlab.us/128.105.144.59:2000",
			wantHost: "c240g5.wisc.cloudlab.us",
			wantIP:   "128.105.144.59",
			wantPort: 2000,
			wantOK:   true,
		},
		{
			name:     "ipv4 without hostname",
			in:       "/127.0.0.1:2001",
			wantHost: "",
			wantIP:   "127.0.0.1",
			wantPort: 2001,
			wantOK:   true,
		},
		{
			name:     "bracketed ipv6",
			in:       "/[2600:1f18:1376:5a02:0:0:0:a]:2000",
			wantHost: "",
			wantIP:   "2600:1f18:1376:5a02:0:0:0:a",
			wantPort: 2000,
			wantOK:   true,
		},
		{
			name:     "bracketed ipv6 with hostname",
			in:       "ar1.xdn.io/[2600:1f18:1376:5a02:0:0:0:a]:2300",
			wantHost: "ar1.xdn.io",
			wantIP:   "2600:1f18:1376:5a02:0:0:0:a",
			wantPort: 2300,
			wantOK:   true,
		},
		{
			name:     "unbracketed ipv6 (older JDK toString)",
			in:       "/2600:1f18:1376:5a02:0:0:0:a:2000",
			wantHost: "",
			wantIP:   "2600:1f18:1376:5a02:0:0:0:a",
			wantPort: 2000,
			wantOK:   true,
		},
		{name: "no slash", in: "127.0.0.1:2000", wantOK: false},
		{name: "no port", in: "/127.0.0.1", wantOK: false},
		{name: "non-numeric port", in: "/127.0.0.1:http", wantOK: false},
		{name: "empty", in: "", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, ip, port, ok := parseNodeSocketAddress(tt.in)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if host != tt.wantHost || ip != tt.wantIP || port != tt.wantPort {
				t.Errorf("got (%q, %q, %d), want (%q, %q, %d)",
					host, ip, port, tt.wantHost, tt.wantIP, tt.wantPort)
			}
		})
	}
}
