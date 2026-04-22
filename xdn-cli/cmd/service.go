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
		successColorPrint := color.New(color.FgGreen).Add(color.Bold).Add(color.Underline)
		errorColorPrint := color.New(color.FgRed).Add(color.Bold).Add(color.Underline)

		err := ValidateControlPlaneConn()
		if err != nil {
			_ = fmt.Errorf("Failed to reach the control plane: %s.\n", err.Error())
			return
		}

		serviceName := args[0]
		fmt.Printf("Getting info for '")
		_, _ = infoColorPrint.Printf("%s", serviceName)
		fmt.Printf("' ...\n")

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
			if httpAddrIf, exists := node["HTTP_ADDRESS"]; exists {
				if httpAddrStr, ok := httpAddrIf.(string); ok && httpAddrStr != "" {
					_, httpIP, httpPort, ok := parseNodeSocketAddress(httpAddrStr)
					if ok {
						httpBaseURL = fmt.Sprintf("http://%s:%d", httpIP, httpPort)
					}
				}
			}
			replicas = append(replicas, placementReplica{
				nodeID:      nodeId,
				host:        host,
				ip:          ip,
				tcpPort:     port,
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

		_, _ = successColorPrint.Printf("SUCCESS")
		fmt.Printf(", current deployment information:\n\n")

		dockerImageName := "unknown"
		consistencyModel := "unknown"
		isDeterministic := "unknown"
		stateDir := "unknown"
		coordinationProtocolName := "unknown"
		var requestBehaviors []interface{}
		if primaryInfo != nil {
			if c := pickStatefulContainer(primaryInfo); c != nil {
				dockerImageName = stringOrDash(c["image"])
			}
			offered := stringOrDash(primaryInfo["consistency"])
			requested := stringOrDash(primaryInfo["requestedConsistency"])
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

		fmt.Printf(" service name  : %s \n", serviceNameStr)
		fmt.Printf(" service url   : http://%s.xdnapp.com/ \n", serviceNameStr)
		fmt.Printf(" docker image  : %s \n", dockerImageName)
		fmt.Printf(" consistency   : %s \n", consistencyModel)
		fmt.Printf(" deterministic : %s \n", isDeterministic)
		fmt.Printf(" state dir.    : %s \n", stateDir)
		fmt.Printf(" num. replica  : %d \n", numReplicas)
		fmt.Printf(" protocol      : %s \n", coordinationProtocolName)
		fmt.Printf("\n")
		fmt.Printf(" Current replicas placement, with epoch=%s:\n", epochNumberStr)
		fmt.Printf("  | %-10s | %-48s | %-10s | %-16s | %-16s |\n",
			"MACHINE ID", "PUBLIC IP", "ROLE", "CREATED", "STATUS")
		for idx, r := range replicas {
			displayAddr := r.ip
			if r.host != "" {
				displayAddr += " (" + r.host + ")"
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
			fmt.Printf("  | %-10s | %-48s | %-10s | %-16s | %-16s |\n",
				r.nodeID, displayAddr, roleStr, createdStr, statusStr)
		}

		if primaryInfo == nil {
			fmt.Printf("\n (control plane reached, but no replica could be contacted)\n")
		}

		if len(requestBehaviors) > 0 {
			fmt.Printf("\n\n")
			fmt.Printf(" Declared service's operation properties:\n")
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
				line := fmt.Sprintf("%-6s %-12s %s", methodsStr, prefix, behavior)
				if name != "" {
					line += "   [" + name + "]"
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
	containersIf, ok := info["containers"].([]interface{})
	if !ok || len(containersIf) == 0 {
		return nil
	}
	if wantName, ok := info["statefulComponent"].(string); ok && wantName != "" {
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

func stringOrDash(v interface{}) string {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return "-"
}
