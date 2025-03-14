package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
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
		dummyColorPrint := color.New(color.FgRed)

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
		numReplicas := 0
		replicaAddressList := make([]string, 0)
		replicaIdList := make([]string, 0)
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
		numReplicas = len(nodes)
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
			rawAddressStr := addressIf.(string)
			nodeId := nodeIdIf.(string)

			// example format: c240g5-110201.wisc.cloudlab.us/128.105.144.59:2000
			rawAddressComponents := strings.Split(rawAddressStr, ":")
			if len(rawAddressComponents) != 2 {
				panic(any("unexpected address format"))
			}
			rawHostComponents := strings.Split(rawAddressComponents[0], "/")
			if len(rawHostComponents) != 2 {
				panic(any("unexpected address host format"))
			}
			host := rawHostComponents[0]
			ipAddress := rawHostComponents[1]
			printedFormat := ipAddress
			if host != "" {
				printedFormat += " (" + host + ")"
			}

			replicaAddressList = append(replicaAddressList, printedFormat)
			replicaIdList = append(replicaIdList, nodeId)
		}

		_, _ = successColorPrint.Printf("SUCCESS")
		fmt.Printf(", current deployment information:\n\n")

		// TODO: query one of the replica to get more about the service details,
		//  for now we are populating dummy data here.
		dockerImageName := dummyColorPrint.Sprint("fadhilkurnia/xdn-bookcatalog")
		httpPort := dummyColorPrint.Sprint("80")
		consistencyModel := dummyColorPrint.Sprint("linearizability")
		isDeterministic := dummyColorPrint.Sprint("true")
		stateDir := dummyColorPrint.Sprint("/app/data/")
		coordinationProtocolName := dummyColorPrint.Sprint("MultiPaxos")

		fmt.Printf(" service name  : %s \n", serviceNameStr)
		fmt.Printf(" service url   : http://%s.xdnapp.com/ \n", serviceNameStr)
		fmt.Printf(" docker image  : %s \n", dockerImageName)
		fmt.Printf(" http port     : %s (internal) \n", httpPort)
		fmt.Printf(" consistency   : %s \n", consistencyModel)
		fmt.Printf(" deterministic : %s \n", isDeterministic)
		fmt.Printf(" state dir.    : %s \n", stateDir)
		fmt.Printf(" num. replica  : %d \n", numReplicas)
		fmt.Printf(" protocol      : %s \n", coordinationProtocolName)
		fmt.Printf("\n")
		fmt.Printf(" Current replicas placement, with epoch=%s:\n",
			epochNumberStr)
		fmt.Printf("  | %-10s | %-48s | %-10s | %-16s | %-16s |\n",
			fmt.Sprint("MACHINE ID"),
			fmt.Sprint("PUBLIC IP"),
			fmt.Sprint("ROLE"),
			fmt.Sprint("CREATED"),
			fmt.Sprint("STATUS"))
		for idx, address := range replicaAddressList {
			roleStr := dummyColorPrint.Sprint("primary")
			createdStr := dummyColorPrint.Sprint("2 minutes ago")
			statusStr := dummyColorPrint.Sprint("Up 2 minutes")
			if idx != 0 {
				roleStr = dummyColorPrint.Sprint("backup ")
			}
			fmt.Printf("  | %-10s | %-48s | %-10s | %-16s | %-16s |\n",
				fmt.Sprintf("%s", replicaIdList[idx]),
				fmt.Sprintf("%s", address),
				fmt.Sprintf("%s   ", roleStr),
				fmt.Sprintf("%s   ", createdStr),
				fmt.Sprintf("%s    ", statusStr))
		}

		fmt.Printf("\n\n")
		fmt.Printf(" Declared service's operation properties:\n")
		fmt.Printf("  - %s\n", dummyColorPrint.Sprint("GET    /*          read-only"))
		fmt.Printf("  - %s\n", dummyColorPrint.Sprint("POST   /*          write-only, read-modify-write"))
		fmt.Printf("  - %s\n", dummyColorPrint.Sprint("PUT    /*          write-only, read-modify-write"))
		fmt.Printf("  - %s\n", dummyColorPrint.Sprint("PATCH  /*          write-only, read-modify-write"))
		fmt.Printf("  - %s\n", dummyColorPrint.Sprint("DELETE /*          write-only, read-modify-write"))
		fmt.Printf("\n")

		// | AR1        | 172.0.0.1  | primary | 2 minutes ago | Up 2 minutes |
		// | AR2        | 172.0.0.2  | backup  |               | Up 2 minutes |
		// | AR3        | 172.0.0.2  | backup  |               | Up 2 minutes |
		//
		// Declared service's operation properties:
		// - GET / read-only

		fmt.Printf("Note that currently %s are still dummy information, "+
			"we are working to implement them\nafter other prioritized research agendas are done.\n",
			dummyColorPrint.Sprint("all texts in red"))

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
			Timeout: 3 * time.Second,
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
