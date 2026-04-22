package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var ServiceRootCmd = &cobra.Command{
	Use:   "service <action>",
	Short: "Does an action toward a launched replicated web services",
}

var ServiceInfoCmd = &cobra.Command{
	Use:   "info <service-name>",
	Short: "Get deployment info about the replicated web service",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		infoColorPrint := color.New(color.FgYellow).Add(color.Bold)
		errorColorPrint := color.New(color.FgRed).Add(color.Bold).Add(color.Underline)
		titleColorPrint := color.New(color.FgWhite).Add(color.Bold)
		columnColorPrint := color.New(color.FgWhite).Add(color.Bold)
		keyColorPrint := color.New(color.FgHiBlue)

		err := ValidateControlPlaneConn()
		if err != nil {
			_ = fmt.Errorf("Failed to reach the control plane: %s.\n", err.Error())
			return
		}

		serviceName := args[0]
		controlPlaneHost := GetControlPlaneHostPort()
		infoEndpoint := fmt.Sprintf("http://%s/api/v2/services/%s/placement",
			controlPlaneHost, serviceName)
		resp, err := http.Get(infoEndpoint)
		if err != nil {
			fmt.Printf(" ")
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			_ = fmt.Errorf("failed to get the deployment info: %s", err.Error())
			return
		}
		if resp.StatusCode != 200 {
			fmt.Printf(" ")
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Failed to get deployment info, received non success code.\n")
			return
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf(" ")
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Failed to get deployment info: \n%s\n", err.Error())
			return
		}

		bodyStr := string(body)
		if strings.Contains(bodyStr, "\"FAILED\":true") {
			fmt.Printf(" ")
			_, _ = errorColorPrint.Printf("ERROR")
			var jsonMap map[string]interface{}
			parseErr := json.Unmarshal([]byte(bodyStr), &jsonMap)
			if parseErr != nil {
				fmt.Printf(" ")
				fmt.Printf("Failed to parse response from control plane.\n")
				return
			}
			fmt.Printf(" ")
			fmt.Printf("Failed to get deployment info:\n")
			errMsgIf := jsonMap["RESPONSE_MESSAGE"]
			errMsg := errMsgIf.(string)
			fmt.Printf(" %s\n", errMsg)
			return
		}

		// try to parse the success response, the example is below
		// {
		//  "DATA": {
		//    "NODES": [
		//      {
		//        "ADDRESS": "/127.0.0.1:2001",
		//        "ID": "AR1",
		//        "ROLE": "replica",										// This is not implemented yet (TODO)
		//        "METADATA": "Created 2 minutes ago; Up 2 minutes", 		// This is not implemented yet (TODO)
		//      },
		//      {
		//        "ADDRESS": "/127.0.0.1:2002",
		//        "ID": "AR2",
		//        "ROLE": "replica",
		//      },
		//      {
		//        "ADDRESS": "/127.0.0.1:2003",
		//        "ID": "AR3",
		//        "ROLE": "replica",
		//      }
		//    ],
		//    "SERVICE_METADATA": "consistency=sequential"					// This is not implemented yet (TODO)
		//  },
		//  "CREATOR": "/127.0.0.1:53343",
		//  "SENDER": "/127.0.0.1:53343",
		//  "IS_QUERY": false,
		//  "EPOCH": 0,
		//  "NAME": "bookcatalog",
		//  "INITIATOR": "/127.0.0.1:53343"
		//}
		var jsonMap map[string]interface{}
		err = json.Unmarshal([]byte(bodyStr), &jsonMap)
		if err != nil {
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			_ = fmt.Errorf("Failed to parse the success response, %s\n",
				err.Error())
			return
		}

		// parse the service name
		serviceNameIf := jsonMap["NAME"]
		serviceNameStr := "?"
		if serviceNameIf != nil {
			if sn, ok := serviceNameIf.(string); ok {
				serviceNameStr = sn
			}
		}

		// parse the placement epoch number
		epochNumberIf := jsonMap["EPOCH"]
		epochNumberStr := "?"
		if epochNumberIf != nil {
			if num, ok := epochNumberIf.(float64); ok {
				epochNumber := int64(num)
				epochNumberStr = fmt.Sprintf("%d", epochNumber)
			}
		}

		// parse the placement data: num replica, ip address, node id, etc
		dataIf := jsonMap["DATA"]
		if dataIf == nil {
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Response from control plane does not contain the placement info\n")
			return
		}
		placementData, ok := dataIf.(map[string]interface{})
		if !ok {
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Response from control plane contains invalid placement data (t=%T)\n",
				dataIf)
			return
		}
		nodesIf, ok := placementData["NODES"]
		if !ok {
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Response from control plane does not have node data\n")
			return
		}
		nodes, ok := nodesIf.([]interface{})
		if !ok {
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Response from control plane contains invalid node format (t=%T)\n",
				nodesIf)
			return
		}

		replicas := make([]placementReplica, 0, len(nodes))
		for _, node := range nodes {
			node, ok := node.(map[string]interface{})
			if !ok {
				continue
			}
			addressIf, addressExist := node["ADDRESS"]
			nodeIdIf, nodeIdExist := node["ID"]
			if !addressExist || !nodeIdExist {
				continue
			}
			addressStr, ok := addressIf.(string)
			if !ok {
				continue
			}
			nodeId, ok := nodeIdIf.(string)
			if !ok {
				continue
			}
			host, ip, port, ok := parseNodeSocketAddress(addressStr)
			if !ok {
				fmt.Fprintf(os.Stderr, "warn: skipping replica %s: unexpected ADDRESS %q\n",
					nodeId, addressStr)
				continue
			}
			httpBaseURL := ""
			httpPort := 0
			if httpAddrIf, exists := node["HTTP_ADDRESS"]; exists {
				if httpAddrStr, ok := httpAddrIf.(string); ok && httpAddrStr != "" {
					_, httpIP, parsedPort, ok := parseNodeSocketAddress(httpAddrStr)
					if ok {
						httpBaseURL = fmt.Sprintf("http://%s:%d", httpIP, parsedPort)
						httpPort = parsedPort
					}
				}
			}
			replicas = append(replicas, placementReplica{
				nodeID:      nodeId,
				host:        host,
				ip:          ip,
				tcpPort:     port,
				httpPort:    httpPort,
				httpBaseURL: httpBaseURL,
			})
		}
		numReplicas := len(replicas)

		// fan out to each AR's /replica/info endpoint (requires XDN header)
		replicaInfos := make([]replicaInfo, numReplicas)
		var wg sync.WaitGroup
		for i, r := range replicas {
			wg.Add(1)
			go func(i int, r placementReplica) {
				defer wg.Done()
				replicaInfos[i] = fetchReplicaInfo(r, serviceName)
			}(i, r)
		}
		wg.Wait()

		// pick the first successful AR response to derive service-wide fields
		var primaryInfo map[string]interface{}
		for _, ri := range replicaInfos {
			if ri.fetchErr == nil && ri.raw != nil {
				primaryInfo = ri.raw
				break
			}
		}

		_, _ = titleColorPrint.Println("Current service deployment information:")

		dockerImageName := "unknown"
		consistencyModel := "unknown"
		isDeterministic := "unknown"
		stateDir := "unknown"
		coordinationProtocolName := "unknown"
		httpPortStr := "unknown"
		var requestBehaviors []interface{}
		if primaryInfo != nil {
			if c := pickStatefulContainer(primaryInfo); c != nil {
				dockerImageName = stringOrDash(c["image"])
			}
			if c := pickEntryContainer(primaryInfo); c != nil {
				if p, ok := c["port"].(float64); ok {
					httpPortStr = strconv.Itoa(int(p))
				}
			}
			offered := toCamelCase(stringOrDash(primaryInfo["consistency"]))
			requested := toCamelCase(stringOrDash(primaryInfo["requestedConsistency"]))
			if requested != "-" && requested != offered {
				consistencyModel = fmt.Sprintf("%s (requested: %s)", offered, requested)
			} else {
				consistencyModel = offered
			}
			if det, ok := primaryInfo["deterministic"].(bool); ok {
				isDeterministic = strconv.FormatBool(det)
			}
			stateDir = stringOrDash(primaryInfo["stateDirectory"])
			coordinationProtocolName = stringOrDash(primaryInfo["protocol"])
			if rb, ok := primaryInfo["requestBehaviors"].([]interface{}); ok {
				requestBehaviors = rb
			}
		}

		fmt.Printf(" %s: %s \n", keyColorPrint.Sprint("Service name "), infoColorPrint.Sprint(serviceNameStr))
		fmt.Printf(" %s: http://%s.xdnapp.com/ *) \n", keyColorPrint.Sprint("Service URL  "), serviceNameStr)
		fmt.Printf(" %s: %s \n", keyColorPrint.Sprint("Docker image "), dockerImageName)
		fmt.Printf(" %s: %s (internal) \n", keyColorPrint.Sprint("HTTP port    "), httpPortStr)
		fmt.Printf(" %s: %s \n", keyColorPrint.Sprint("Consistency  "), consistencyModel)
		fmt.Printf(" %s: %s \n", keyColorPrint.Sprint("Deterministic"), isDeterministic)
		fmt.Printf(" %s: %s \n", keyColorPrint.Sprint("State dir.   "), stateDir)
		fmt.Printf(" %s: %d \n", keyColorPrint.Sprint("Num. replica "), numReplicas)
		fmt.Printf(" %s: %s \n", keyColorPrint.Sprint("Protocol     "), coordinationProtocolName)
		footnoteColorPrint := color.New(color.FgHiBlack)
		fmt.Printf(" %s\n", footnoteColorPrint.Sprint("*) service URL is reachable once xdn-dns is configured in the control plane"))
		fmt.Printf("\n")
		fmt.Printf(" %s\n", titleColorPrint.Sprintf("Current replicas placement (epoch=%s):", epochNumberStr))

		type replicaRow struct {
			machineID, ipAddress, webPort, role, created, status string
		}
		rows := make([]replicaRow, len(replicas))
		for idx, r := range replicas {
			displayAddr := r.ip
			if r.host != "" {
				displayAddr += " (" + r.host + ")"
			}
			webPortStr := "-"
			if r.httpPort != 0 {
				webPortStr = strconv.Itoa(r.httpPort)
			}
			roleStr := "unreachable"
			createdStr := "unreachable"
			statusStr := "unreachable"
			info := replicaInfos[idx]
			if info.fetchErr == nil && info.raw != nil {
				roleStr = stringOrDash(info.raw["role"])
				if c := pickStatefulContainer(info.raw); c != nil {
					createdStr = stringOrDash(c["createdAt"])
					statusStr = stringOrDash(c["status"])
				}
			}
			rows[idx] = replicaRow{r.nodeID, displayAddr, webPortStr, roleStr, createdStr, statusStr}
		}
		wID, wIP, wPort := len("NODE ID"), len("IP ADDRESS"), len("WEB PORT")
		wRole, wCreated, wStatus := len("ROLE"), len("CREATED"), len("STATUS")
		for _, row := range rows {
			if n := len(row.machineID); n > wID {
				wID = n
			}
			if n := len(row.ipAddress); n > wIP {
				wIP = n
			}
			if n := len(row.webPort); n > wPort {
				wPort = n
			}
			if n := len(row.role); n > wRole {
				wRole = n
			}
			if n := len(row.created); n > wCreated {
				wCreated = n
			}
			if n := len(row.status); n > wStatus {
				wStatus = n
			}
		}
		fmt.Printf("  | %s | %s | %s | %s | %s | %s |\n",
			columnColorPrint.Sprintf("%-*s", wID, "NODE ID"),
			columnColorPrint.Sprintf("%-*s", wIP, "IP ADDRESS"),
			columnColorPrint.Sprintf("%-*s", wPort, "WEB PORT"),
			columnColorPrint.Sprintf("%-*s", wRole, "ROLE"),
			columnColorPrint.Sprintf("%-*s", wCreated, "CREATED"),
			columnColorPrint.Sprintf("%-*s", wStatus, "STATUS"))
		for _, row := range rows {
			roleCell := colorForRole(row.role).Sprint(fmt.Sprintf("%-*s", wRole, row.role))
			statusCell := colorForStatus(row.status).Sprint(fmt.Sprintf("%-*s", wStatus, row.status))
			fmt.Printf("  | %-*s | %-*s | %-*s | %s | %-*s | %s |\n",
				wID, row.machineID, wIP, row.ipAddress, wPort, row.webPort,
				roleCell, wCreated, row.created, statusCell)
		}

		if primaryInfo == nil {
			fmt.Printf("\n (control plane reached, but no replica could be contacted)\n")
		}

		if len(requestBehaviors) > 0 {
			type behaviorRow struct {
				methods, prefix, behavior, name string
			}
			rows := make([]behaviorRow, 0, len(requestBehaviors))
			methodsW, prefixW := 0, 0
			for _, b := range requestBehaviors {
				bm, ok := b.(map[string]interface{})
				if !ok {
					continue
				}
				methodsStr := "*"
				if ms, ok := bm["methods"].([]interface{}); ok && len(ms) > 0 {
					parts := make([]string, 0, len(ms))
					for _, m := range ms {
						if s, ok := m.(string); ok {
							parts = append(parts, s)
						}
					}
					if len(parts) > 0 {
						methodsStr = strings.Join(parts, ",")
					}
				}
				prefix := stringOrDash(bm["prefix"])
				behavior := stringOrDash(bm["behavior"])
				name, _ := bm["name"].(string)
				rows = append(rows, behaviorRow{methodsStr, prefix, behavior, name})
				if len(methodsStr) > methodsW {
					methodsW = len(methodsStr)
				}
				if len(prefix) > prefixW {
					prefixW = len(prefix)
				}
			}

			fmt.Printf("\n\n")
			fmt.Printf(" %s\n", titleColorPrint.Sprint("Declared service's operation behaviors:"))
			for _, r := range rows {
				line := fmt.Sprintf("%-*s  %-*s  %s", methodsW, r.methods, prefixW, r.prefix, r.behavior)
				if r.name != "" {
					line += "   [" + r.name + "]"
				}
				fmt.Printf("  - %s\n", line)
			}
		}

		fmt.Printf("\n")
	},
}

var ServiceDestroyCmd = &cobra.Command{
	Use:   "destroy <service-name>",
	Short: "Permanently remove a replicated web service from edge servers",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		infoColorPrint := color.New(color.FgYellow).Add(color.Bold)
		successColorPrint := color.New(color.FgGreen).Add(color.Bold).Add(color.Underline)
		errorColorPrint := color.New(color.FgRed).Add(color.Bold).Add(color.Underline)

		err := ValidateControlPlaneConn()
		if err != nil {
			_ = fmt.Errorf("Failed to reach the control plane: %s.\n", err.Error())
			return fmt.Errorf("failed to reach the control plane: %s", err.Error())
		}

		serviceName := args[0]
		isValidInput := false
		isRemoveConfirmed := false
		for !isValidInput {
			fmt.Printf(
				"Are you sure you want to remove `%s` service? [yes/no]\n > ",
				serviceName)
			input := bufio.NewScanner(os.Stdin)
			input.Scan()
			isSureText := input.Text()
			if isSureText == "yes" || isSureText == "no" {
				isValidInput = true
				if isSureText == "yes" {
					isRemoveConfirmed = true
				}
				break
			}
			fmt.Printf(
				"  '%s' is not a valid answer, "+
					"please answer with 'yes' or 'no'.\n",
				isSureText)
		}

		if !isRemoveConfirmed {
			fmt.Printf("  '%s' is not removed.\n", serviceName)
			return fmt.Errorf("'%s' is not removed", serviceName)
		}

		controlPlaneHost := GetControlPlaneHostPort()
		deleteEndpoint := fmt.Sprintf("http://%s/?type=DELETE&name=%s",
			controlPlaneHost, serviceName)
		fmt.Printf("Removing service '")
		_, _ = infoColorPrint.Printf("%s", serviceName)
		fmt.Printf("' ...\n")
		client := http.Client{
			Timeout: 60 * time.Second,
		}
		resp, err := client.Get(deleteEndpoint)
		if err != nil {
			fmt.Printf(" ")
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Println("request timed out:", err)
				os.Exit(100)
				return netErr
			}
			fmt.Printf("Failed to destroy the service, error: %s", err.Error())
			return fmt.Errorf("failed to destroy the service, error: %s", err.Error())
		}
		if resp.StatusCode != 200 {
			fmt.Printf(" ")
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Failed to remove the service, received non success code.\n")
			return fmt.Errorf("failed to remove the service, received http non success code %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf(" ")
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Failed to remove the service: \n%s\n", err.Error())
			return fmt.Errorf("failed to remove the service: %s", err.Error())
		}

		bodyStr := string(body)
		if strings.Contains(bodyStr, "\"FAILED\":true") {
			fmt.Printf(" ")
			_, _ = errorColorPrint.Printf("ERROR")
			fmt.Printf(" ")
			fmt.Printf("Failed to remove the service: \n")
			var jsonMap map[string]interface{}
			_ = json.Unmarshal([]byte(bodyStr), &jsonMap)
			errMsgIf := jsonMap["RESPONSE_MESSAGE"]
			errMsg := errMsgIf.(string)
			fmt.Printf(" %s\n", errMsg)
			return fmt.Errorf("failed to remove the service: %s", errMsg)
		}

		_, _ = successColorPrint.Printf("SUCCESS")
		fmt.Printf(": service ")
		_, _ = infoColorPrint.Printf("%s", serviceName)
		fmt.Printf(" is removed successfully.\n")

		return nil
	},
}

func init() {
	ServiceRootCmd.AddCommand(ServiceDestroyCmd)
	ServiceRootCmd.AddCommand(ServiceInfoCmd)
}

type placementReplica struct {
	nodeID      string
	host        string
	ip          string
	tcpPort     int
	httpPort    int    // 0 if HTTP_ADDRESS missing from the RC response
	httpBaseURL string // empty if HTTP_ADDRESS missing from the RC response
}

type replicaInfo struct {
	raw      map[string]interface{}
	fetchErr error
}

// parseNodeSocketAddress parses Java InetSocketAddress.toString() output
// like "c240g5.wisc.cloudlab.us/128.105.144.59:2000" or "/127.0.0.1:2001".
// Returns host (may be empty), ip, port; ok=false on malformed input.
func parseNodeSocketAddress(s string) (host, ip string, port int, ok bool) {
	hostPort := strings.SplitN(s, ":", 2)
	if len(hostPort) != 2 {
		return "", "", 0, false
	}
	hostIP := strings.SplitN(hostPort[0], "/", 2)
	if len(hostIP) != 2 {
		return "", "", 0, false
	}
	p, err := strconv.Atoi(hostPort[1])
	if err != nil {
		return "", "", 0, false
	}
	return hostIP[0], hostIP[1], p, true
}

// fetchReplicaInfo queries the AR's /replica/info endpoint. The AR HTTP
// frontend requires the XDN header to route into XDN-specific handling.
func fetchReplicaInfo(r placementReplica, serviceName string) replicaInfo {
	if r.httpBaseURL == "" {
		return replicaInfo{fetchErr: fmt.Errorf("no HTTP address from control plane")}
	}
	url := fmt.Sprintf("%s/api/v2/services/%s/replica/info", r.httpBaseURL, serviceName)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return replicaInfo{fetchErr: err}
	}
	req.Header.Set("XDN", serviceName)
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return replicaInfo{fetchErr: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return replicaInfo{fetchErr: err}
	}
	if resp.StatusCode != http.StatusOK {
		return replicaInfo{fetchErr: fmt.Errorf("http %d", resp.StatusCode)}
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return replicaInfo{fetchErr: err}
	}
	return replicaInfo{raw: raw}
}

// pickStatefulContainer returns the containers[] entry whose "name" matches
// info["statefulComponent"], falling back to containers[0]. Returns nil if
// the response has no container array.
func pickStatefulContainer(info map[string]interface{}) map[string]interface{} {
	return pickNamedContainer(info, "statefulComponent")
}

// pickEntryContainer returns the containers[] entry whose "name" matches
// info["entryComponent"], falling back to containers[0].
func pickEntryContainer(info map[string]interface{}) map[string]interface{} {
	return pickNamedContainer(info, "entryComponent")
}

func pickNamedContainer(info map[string]interface{}, nameKey string) map[string]interface{} {
	containersIf, ok := info["containers"].([]interface{})
	if !ok || len(containersIf) == 0 {
		return nil
	}
	if wantName, ok := info[nameKey].(string); ok && wantName != "" {
		for _, c := range containersIf {
			if cm, ok := c.(map[string]interface{}); ok {
				if n, _ := cm["name"].(string); n == wantName {
					return cm
				}
			}
		}
	}
	if cm, ok := containersIf[0].(map[string]interface{}); ok {
		return cm
	}
	return nil
}

// colorForRole highlights the leader-equivalent role (leader/coordinator/
// primary) in bold and dims secondary roles (follower/backup/replica/…).
// "unreachable" stays red so it matches the status cell signalling.
func colorForRole(s string) *color.Color {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "leader", "coordinator", "primary":
		return color.New(color.FgGreen)
	case "unreachable":
		return color.New(color.FgRed)
	default:
		return color.New(color.FgHiBlack)
	}
}

// colorForStatus picks a color for a container status / replica reachability
// string. Healthy states go green, failure states go red; anything else
// renders in the terminal default.
func colorForStatus(s string) *color.Color {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "running":
		return color.New(color.FgGreen)
	case "unreachable", "exited", "dead":
		return color.New(color.FgRed)
	case "created", "starting", "restarting", "paused":
		return color.New(color.FgYellow)
	default:
		return color.New()
	}
}

func stringOrDash(v interface{}) string {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return "-"
}

// toCamelCase converts SCREAMING_SNAKE_CASE (as emitted by ConsistencyModel
// enum values like LINEARIZABILITY, READ_YOUR_WRITES) to camelCase:
// "LINEARIZABILITY" → "linearizability", "READ_YOUR_WRITES" → "readYourWrites".
// Inputs that are already lowercased or the sentinel "-" pass through.
func toCamelCase(s string) string {
	if s == "" || s == "-" {
		return s
	}
	parts := strings.Split(s, "_")
	var b strings.Builder
	for i, p := range parts {
		if p == "" {
			continue
		}
		p = strings.ToLower(p)
		if i == 0 {
			b.WriteString(p)
			continue
		}
		b.WriteString(strings.ToUpper(p[:1]))
		b.WriteString(p[1:])
	}
	return b.String()
}
