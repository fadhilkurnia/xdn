package xdn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	maxMindDB "github.com/oschwald/maxminddb-golang/v2"
)

const pluginName = "xdn"
const controlPlanePort = 3300
const responseTTLSec = 1
const geoLocationDbFilePath = "/tmp/geo/geolocation_city_data.mmdb"

// XDN is a plugin that return a set of IP address where a replica group is
// deployed in XDN. XDN plugin implements the `plugin.Handler` interface.
type XDN struct {
	plugin.Handler

	xdnControlPlaneHost      string // Example: xp.xdnapp.com
	xdnControlPlaneIPAddress string // Assuming we are the Control Plane

	// node id -> global IPv6, for the reserved `edge` sub-zone (per-replica
	// direct addressing: <nodeid>.edge.<base_domain> -> that node's IPv6).
	edgeNodes map[string]string

	geoLocationDb *maxMindDB.Reader
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

	// if service name is not specified, we refuse to answer
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
		rr.(*dns.A).A = net.ParseIP(x.xdnControlPlaneIPAddress).To4()
		a.Answer = []dns.RR{rr}
		err := w.WriteMsg(a)
		if err != nil {
			return dns.RcodeServerFailure, err
		}

		return dns.RcodeSuccess, nil
	}

	// Reserved `edge` sub-zone: <nodeid>.edge.<base_domain> resolves DIRECTLY to
	// that specific node's GLOBAL IPv6 (no geo-routing), so a browser can reach a
	// chosen replica over a cert-valid name (the wildcard *.edge.<base_domain>
	// cert). serviceName here is the label before the base domain == "edge".
	if serviceName == "edge" && len(domainParts) >= 5 {
		nodeID := domainParts[len(domainParts)-5]
		// IPv6-only (like service names): answer AAAA; NODATA for other qtypes.
		if state.QType() != dns.TypeAAAA {
			if err := w.WriteMsg(a); err != nil {
				return dns.RcodeServerFailure, err
			}
			return dns.RcodeSuccess, nil
		}
		ipv6, ok := x.edgeNodes[nodeID]
		if !ok {
			if err := w.WriteMsg(a); err != nil {
				return dns.RcodeServerFailure, err
			}
			return dns.RcodeNameError, nil
		}
		rr := new(dns.AAAA)
		rr.Hdr = dns.RR_Header{
			Name:   state.QName(),
			Rrtype: dns.TypeAAAA,
			Class:  state.QClass(),
			Ttl:    responseTTLSec,
		}
		rr.AAAA = net.ParseIP(ipv6).To16()
		a.Answer = []dns.RR{rr}
		if err := w.WriteMsg(a); err != nil {
			return dns.RcodeServerFailure, err
		}
		fmt.Println(">> edge node", nodeID, "->", ipv6)
		return dns.RcodeSuccess, nil
	}

	// All service name must be longer than 3 chars since subdomain with
	// 1 or 2 chars are reserved for xdn operational purposes.
	// For example:
	// - rc.xdnapp.com for the Reconfigurator (i.e., Control Plane)
	// - cp.xdnapp.com for the Reconfigurator as well
	// - ns.xdnapp.com for the Name Server
	if len(serviceName) <= 3 {

		myIPAddress := x.xdnControlPlaneIPAddress

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
			err := w.WriteMsg(a)
			if err != nil {
				return dns.RcodeServerFailure, err
			}

			fmt.Println(">> returning my own IP as Control Plane: ", myIPAddress)

			return dns.RcodeSuccess, nil
		}

		return dns.RcodeRefused, nil
	}

	// Service names resolve to the AR's GLOBAL IPv6 (consensus advertises IPv6,
	// so getIPSets returns IPv6 addresses). We answer AAAA only; an A query gets
	// NODATA (empty NOERROR) so clients connect over IPv6.
	if state.QType() != dns.TypeAAAA {
		if err := w.WriteMsg(a); err != nil {
			return dns.RcodeServerFailure, err
		}
		return dns.RcodeSuccess, nil
	}

	// send request to RC
	IPs := x.getIPSets(serviceName)
	if len(IPs) == 0 {
		if err := w.WriteMsg(a); err != nil {
			return dns.RcodeServerFailure, err
		}
		return dns.RcodeNameError, nil
	}
	fmt.Println(">> IPs: ", IPs)

	clientIP := state.IP()
	fmt.Println(">> Client IP: ", clientIP)
	resultedIP, err := x.pickClosestIP(IPs, clientIP)
	if err != nil {
		fmt.Println(">> failed to get closest IP: " + err.Error() + ". Fallback to random pick.")
		resultedIP = x.pickRandomIP(IPs)
	}
	fmt.Println(">> IP: ", resultedIP)

	// prepare and send the AAAA record
	rr := new(dns.AAAA)
	rr.Hdr = dns.RR_Header{
		Name:   state.QName(),
		Rrtype: dns.TypeAAAA,
		Class:  state.QClass(),
		Ttl:    responseTTLSec,
	}
	rr.AAAA = net.ParseIP(resultedIP).To16()
	a.Answer = []dns.RR{rr}
	if err = w.WriteMsg(a); err != nil {
		return dns.RcodeServerFailure, err
	}

	return dns.RcodeSuccess, nil
}

// Sends request to XDN's control plane. Returns list of IP address of the given
// name, if any. If error occured, an empty list is returned (fail open).
func (x *XDN) getIPSets(serviceName string) []string {
	url := fmt.Sprintf("http://%s:%d/?TYPE=REQ_ACTIVES&name=%s",
		x.xdnControlPlaneHost, controlPlanePort, serviceName)
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
	var IPs []string
	if respJSON["ACTIVE_REPLICAS"] != nil {
		actives := respJSON["ACTIVE_REPLICAS"].([]interface{})
		// example formats: "p113.utah.cloudlab.us/127.0.0.1:2000"
		//                  "host/[2001:db8::1]:2000"  or  "/[2001:db8::1]:2000"
		for i := 0; i < len(actives); i++ {
			rawString := actives[i].(string)
			ip := extractIP(rawString)
			if ip == "" {
				fmt.Println(">> error, invalid address format received:" +
					rawString)
				return nil
			}
			IPs = append(IPs, ip)
		}
	}

	return IPs
}

// extractIP pulls the bare IP (IPv4 or IPv6) out of a gigapaxos address string
// like "host/1.2.3.4:2000" or "host/[2001:db8::1]:2000". Splitting on ":" is
// wrong for IPv6, so we drop the "host/" prefix and the trailing ":port",
// honoring the [..] brackets around an IPv6 literal.
func extractIP(raw string) string {
	if i := strings.LastIndex(raw, "/"); i >= 0 {
		raw = raw[i+1:]
	}
	if strings.HasPrefix(raw, "[") {
		if j := strings.Index(raw, "]"); j > 0 {
			return raw[1:j]
		}
		return ""
	}
	if i := strings.LastIndex(raw, ":"); i >= 0 {
		return raw[:i]
	}
	return raw
}

// Picks one random IP from the list of IP Address. Assume IPs is not empty.
func (x *XDN) pickRandomIP(IPs []string) string {
	if len(IPs) == 0 {
		return ""
	}
	return IPs[rand.Intn(len(IPs))]
}

// Picks the closest IP, geographically, given the client IP using the cached
// maxmind geoIP database. Assume IPs is not empty.
// Return an empty string on error.
func (x *XDN) pickClosestIP(IPs []string, clientIP string) (string, error) {
	if len(IPs) == 0 {
		return "", errors.New("IPs cannot be empty")
	}

	// Handle edge case when there is only one IP provided.
	result := IPs[0]
	if len(IPs) == 1 {
		return result, nil
	}

	if x.geoLocationDb == nil {
		db, err := maxMindDB.Open(geoLocationDbFilePath)
		if err != nil {
			return "", err
		}
		x.geoLocationDb = db
	}
	db := x.geoLocationDb

	type GeoLocation struct {
		Location struct {
			Latitude  float64 `maxminddb:"latitude"`
			Longitude float64 `maxminddb:"longitude"`
		} `maxminddb:"location"`
	}

	// parse the requester IP and get its geolocation
	clientAddress := netip.MustParseAddr(clientIP)
	var clientLocation GeoLocation
	err := db.Lookup(clientAddress).Decode(&clientLocation)
	if err != nil {
		return "", err
	}

	// parse the IPs and get their geolocations
	var locationOptions []GeoLocation
	for _, IP := range IPs {
		addr := netip.MustParseAddr(IP)
		var location GeoLocation
		err = db.Lookup(addr).Decode(&location)
		if err != nil {
			return "", err
		}
		locationOptions = append(locationOptions, location)
	}

	// calculate the closest IP address, based on the parsed location
	closestIdx := 0
	closestDistance := math.Inf(1)
	for idx, loc := range locationOptions {
		currDistance := calculateDistance(loc.Location.Latitude, loc.Location.Longitude,
			clientLocation.Location.Latitude, clientLocation.Location.Longitude)
		if currDistance < closestDistance {
			closestIdx = idx
			closestDistance = currDistance
		}
	}

	return IPs[closestIdx], nil
}

// The distance is returned in kilometers.
func calculateDistance(lat1 float64, lon1 float64, lat2 float64, lon2 float64) float64 {
	const earthRadius = 6371 // Earth's radius in kilometers

	// Convert latitude and longitude from degrees to radians.
	lat1Rad := lat1 * math.Pi / 180
	lon1Rad := lon1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	lon2Rad := lon2 * math.Pi / 180

	// Compute differences between points.
	deltaLat := lat2Rad - lat1Rad
	deltaLon := lon2Rad - lon1Rad

	// Apply the Haversine formula.
	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	// Calculate the distance.
	distance := earthRadius * c
	return distance
}
