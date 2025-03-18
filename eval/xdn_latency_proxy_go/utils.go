package main

import (
	"bufio"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
)

// ServerLocation holds the server's name and geographic coordinates.
type ServerLocation struct {
	Name      string
	HostPort  string
	Latitude  float64
	Longitude float64
}

// Config holds everything loaded from the JSON config file.
type Config struct {
	ServerLocations map[string]ServerLocation `json:"server_locations"`
	SlowdownFactor  float64                   `json:"slowdown_factor"`
}

// readConfig parses a .properties‐style file. It expects lines of the form
//
//	active.bos=10.10.1.1:2000
//	active.bos.geolocation=42.354,-71.061
//	XDN_EVAL_LATENCY_SLOWDOWN_FACTOR=0.32258064516
func readConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// We'll collect partial data in this structure, then build our final Config.
	type serverProps struct {
		Name     string
		HostPort string
		Lat      float64
		Lon      float64
	}

	serversMap := make(map[string]*serverProps)
	slowdown := 1.0 // default if not specified

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			// Skip blank lines and comments
			continue
		}

		// Split on the first '='
		eqIndex := strings.Index(line, "=")
		if eqIndex < 0 {
			continue // not a valid key=value line
		}

		key := strings.TrimSpace(line[:eqIndex])
		val := strings.TrimSpace(line[eqIndex+1:])

		// 1. Check for slowdown factor
		if key == "XDN_EVAL_LATENCY_SLOWDOWN_FACTOR" {
			if f, err := strconv.ParseFloat(val, 64); err == nil {
				slowdown = f
			} else {
				log.Printf("Warning: could not parse slowdown factor '%s': %v\n", val, err)
			}
			continue
		}

		// 2. Check for active.* keys
		if strings.HasPrefix(key, "active.") {
			// remove "active." prefix
			rest := key[len("active."):] // e.g. "bos=...", "bos.geolocation=..."

			// If it ends with ".geolocation", parse lat/long from val
			if strings.HasSuffix(rest, ".geolocation") {
				serverName := strings.TrimSuffix(rest, ".geolocation") // e.g. "bos"
				latlon := strings.Split(val, ",")
				if len(latlon) == 2 {
					latStr := strings.TrimSpace(latlon[0])
					lonStr := strings.TrimSpace(latlon[1])
					latF, err1 := strconv.ParseFloat(latStr, 64)
					lonF, err2 := strconv.ParseFloat(lonStr, 64)
					if err1 == nil && err2 == nil {
						if _, exists := serversMap[serverName]; !exists {
							serversMap[serverName] = &serverProps{Name: serverName}
						}
						serversMap[serverName].Lat = latF
						serversMap[serverName].Lon = lonF
					} else {
						log.Printf("Warning: invalid geolocation format for %s: %s\n", serverName, val)
					}
				} else {
					log.Printf("Warning: invalid geolocation format: %s\n", val)
				}
			} else {
				// Otherwise, it's the host:port value, e.g. active.bos=10.10.1.1:2000
				serverName := rest // "bos"
				if _, exists := serversMap[serverName]; !exists {
					serversMap[serverName] = &serverProps{Name: serverName}
				}
				serversMap[serverName].HostPort = val
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Build our final serverLocations map, keyed by the host:port from the file.
	serverLocations := make(map[string]ServerLocation)
	for _, s := range serversMap {
		if s.HostPort == "" {
			continue // skip anything missing host:port
		}
		serverLocations[s.HostPort] = ServerLocation{
			Name:      s.Name,
			HostPort:  s.HostPort,
			Latitude:  s.Lat,
			Longitude: s.Lon,
		}
	}

	cfg := &Config{
		ServerLocations: serverLocations,
		SlowdownFactor:  slowdown,
	}
	return cfg, nil
}

// calculates the distance in meters between two lat/long points.
func calculateHaversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadius = 6371000.0 // meters

	// Convert degrees to radians
	lat1Rad := lat1 * math.Pi / 180.0
	lon1Rad := lon1 * math.Pi / 180.0
	lat2Rad := lat2 * math.Pi / 180.0
	lon2Rad := lon2 * math.Pi / 180.0

	// Apply the Haversine formula
	dLat := lat2Rad - lat1Rad
	dLon := lon2Rad - lon1Rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	// Distance in meters
	return earthRadius * c
}

// returns the one-way latency in milliseconds for a given distance and slowdown factor.
func getEmulatedLatency(distanceMeters float64, slowdownFactor float64) float64 {
	if slowdownFactor <= 0.0 || slowdownFactor > 1.0 {
		panic("invalid slowdown factor")
	}

	// Speed of light in m/s
	const speedOfLight = 299792458.0

	// time in seconds for one-way light travel
	effectiveSpeed := speedOfLight * slowdownFactor
	timeSeconds := distanceMeters / effectiveSpeed

	// Convert to milliseconds
	return timeSeconds * 1000.0
}
