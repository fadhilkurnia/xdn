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
		err := w.WriteMsg(a)
		if err != nil {
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

	rr.(*dns.A).A = net.ParseIP(resultedIP).To4()

	// put the record into the answer, then send it
	a.Answer = []dns.RR{rr}
	err = w.WriteMsg(a)
	if err != nil {
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
		// example format: "p113.utah.cloudlab.us/127.0.0.1:2000"
		for i := 0; i < len(actives); i++ {
			rawString := actives[i].(string)
			rawHostPort := strings.Split(rawString, ":")
			if len(rawHostPort) != 2 {
				fmt.Println(">> error, invalid address format received:" +
					rawString)
				return nil
			}
			rawHost := strings.Split(rawHostPort[0], "/")
			if len(rawHostPort) != 2 {
				fmt.Println(">> error, invalid address host format received:" +
					rawHostPort[0])
				return nil
			}
			IPs = append(IPs, rawHost[1])
		}
	}

	return IPs
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
