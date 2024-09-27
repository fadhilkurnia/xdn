package xdn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
)

const pluginName = "xdn"
const controlPlanePort = 3300
const responseTTLSec = 1

// XDN is a plugin that return a set of IP address where a replica group is
// deployed in XDN. XDN plugin implements the `plugin.Handler` interface.
type XDN struct {
	plugin.Handler

	myControlPlane string
	myIPAddress    string
}

// Name implements the `plugin.Handler` interface, returning the name
// of the plugin: "xdn".
func (x XDN) Name() string { return pluginName }

// ServeDNS implements the `plugin.Handler` interface, handling an incoming
// DNS request by querying into the XDN control plane.
func (x XDN) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}

	// prepare the response
	a := new(dns.Msg)
	a.SetReply(r)
	a.Authoritative = true

	// get IP address of the client
	ip := state.IP()
	fmt.Println(">> client IP: ", ip)

	// get the asked name
	var serviceName string
	fmt.Println(">> name: ", state.Name())
	domainParts := strings.Split(state.Name(), ".")
	fmt.Println(">> num. domain part: ", len(domainParts))
	if len(domainParts) > 3 {
		serviceName = domainParts[len(domainParts)-4]
		fmt.Println(">> service name: ", serviceName)
	}

	// if service name is not specified, we refused to answer
	// the dns query
	if len(domainParts) <= 3 {
		var rr dns.RR
		rr = new(dns.A)
		rr.(*dns.A).Hdr = dns.RR_Header{
			Name:   state.QName(),
			Rrtype: dns.TypeA,
			Class:  state.QClass(),
			Ttl:    60,
		}
		rr.(*dns.A).A = net.ParseIP(x.myIPAddress).To4()
		a.Answer = []dns.RR{rr}
		w.WriteMsg(a)

		return dns.RcodeSuccess, nil
	}

	// All service name must be longer than 3 chars since subdomain with
	// 1 or 2 chars are reserved for xdn operational purposes.
	// For example:
	// - rc.xdnapp.com for the Reconfigurator (i.e., Control Plane)
	// - cp.xdnapp.com for the Reconfigurator as well
	// - ns.xdnapp.com for the Name Server
	if len(serviceName) <= 3 {

		myIPAddress := x.myIPAddress

		if serviceName == "rc" || serviceName == "cp" || serviceName == "ns" ||
			serviceName == "ns1" || serviceName == "ns2" {
			var rr dns.RR
			rr = new(dns.A)
			rr.(*dns.A).Hdr = dns.RR_Header{
				Name:   state.QName(),
				Rrtype: dns.TypeA,
				Class:  state.QClass(),
				Ttl:    60,
			}
			rr.(*dns.A).A = net.ParseIP(myIPAddress).To4()
			a.Answer = []dns.RR{rr}
			w.WriteMsg(a)

			return dns.RcodeSuccess, nil
		}

		return dns.RcodeRefused, nil
	}

	// prepare the A record to be sent
	var rr dns.RR
	rr = new(dns.A)
	rr.(*dns.A).Hdr = dns.RR_Header{
		Name:   state.QName(),
		Rrtype: dns.TypeA,
		Class:  state.QClass(),
		Ttl:    responseTTLSec,
	}

	// send request to RC
	IPs := x.getIPSets(serviceName)
	if IPs == nil || len(IPs) == 0 {
		a.Answer = []dns.RR{rr}
		w.WriteMsg(a)
		return dns.RcodeNameError, nil
	}
	fmt.Println(">> IPs: ", IPs)

	resultedIP := x.pickIP(IPs)
	fmt.Println(">> IP: ", resultedIP)

	rr.(*dns.A).A = net.ParseIP(resultedIP).To4()

	// put the record into the answer, then send it
	a.Answer = []dns.RR{rr}
	w.WriteMsg(a)

	return dns.RcodeSuccess, nil
}

// Sends request to XDN's control plane. Returns list of IP address of the given
// name, if any. If error occured, an empty list is returned (fail open).
func (x XDN) getIPSets(serviceName string) []string {
	url := fmt.Sprintf("http://%s:%d/?TYPE=REQ_ACTIVES&name=%s",
		x.myControlPlane, controlPlanePort, serviceName)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println(">> error, failed to contact RC: ", err)
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Println(">> error, got non OK status: ", resp.StatusCode)
		return nil
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(">> error, failed to parse response: ", respBody)
		return nil
	}

	respJSON := map[string]interface{}{}
	err = json.Unmarshal(respBody, &respJSON)
	if err != nil {
		fmt.Println(">> error, failed to unmarshal JSON: ", err)
		return nil
	}
	if respJSON["FAILED"] != nil {
		isFailed := respJSON["FAILED"].(bool)
		if isFailed {
			fmt.Println(">> error, got failed response.")
			return nil
		}
	}

	// parse the returned IPs
	IPs := []string{}
	if respJSON["ACTIVE_REPLICAS"] != nil {
		actives := respJSON["ACTIVE_REPLICAS"].([]interface{})
		// example format: "/127.0.0.1:2000"
		for i := 0; i < len(actives); i++ {
			rawString := actives[i].(string)
			rawIPPort := strings.Split(rawString, ":")
			rawIP := rawIPPort[0]
			IPs = append(IPs, rawIP[1:len(rawIP)])
		}
	}

	return IPs
}

// Picks one random IP from the list of IP Address.
// TODO: pick based on the proximity to the client's IP address instead.
func (x XDN) pickIP(IPs []string) string {
	return IPs[rand.Intn(len(IPs))]
}
