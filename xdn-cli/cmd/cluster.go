// Package cmd defines the Cobra command tree for the xdn CLI.
//
// cluster.go adds the `xdn cluster` command group, whose `launch` subcommand deploys a
// *self-clustering* service (analogous to a Kubernetes StatefulSet). Unlike `xdn launch`,
// where XDN performs the replication itself, a cluster service handles its own coordination
// inside the containers (e.g. an etcd ensemble, a Postgres replication cluster). XDN's job
// reduces to: placement, stable ordinal identity, a swarm overlay for peer discovery, and
// routing client traffic to a replica's local container.
package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

// ClusterRootCmd is the parent of all `xdn cluster *` subcommands.
var ClusterRootCmd = &cobra.Command{
	Use:   "cluster <action>",
	Short: "Deploy and manage self-clustering replicated services (StatefulSet-style)",
}

// ClusterLaunchCmd implements `xdn cluster launch <name>`.
var ClusterLaunchCmd = &cobra.Command{
	Use:   "launch <service-name>",
	Short: "Launch a self-clustering service (the container handles its own coordination)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		serviceName := args[0]

		image, _ := cmd.Flags().GetString("image")
		httpPort, _ := cmd.Flags().GetInt("port")
		peerPort, _ := cmd.Flags().GetInt("peer-port")
		stateDir, _ := cmd.Flags().GetString("state")
		numReplicas, _ := cmd.Flags().GetInt("num-replicas")
		adapter, _ := cmd.Flags().GetString("adapter")
		rawEnv, _ := cmd.Flags().GetStringArray("env")

		if err := validateServiceName(serviceName); err != nil {
			return err
		}
		if err := validateImageName(image); err != nil {
			return err
		}
		if peerPort < 1 || peerPort > 65535 {
			return fmt.Errorf("--peer-port must be in 1..65535 (got %d)", peerPort)
		}
		if numReplicas < 1 {
			return fmt.Errorf("--num-replicas must be >= 1 for a cluster service")
		}
		if stateDir == "" || !strings.HasSuffix(stateDir, "/") {
			return fmt.Errorf("--state must be a non-empty absolute path ending with '/'")
		}

		// Build the JSON spec. mode:cluster is the only deviation from a regular launch;
		// the validator and reconfigurator-side extractor read the same xdn:init: prefix.
		config := map[string]interface{}{
			"name":         serviceName,
			"image":        image,
			"port":         httpPort,
			"state":        stateDir,
			"mode":         "cluster",
			"peer_port":    peerPort,
			"num_replicas": numReplicas,
		}
		if adapter != "" {
			config["adapter"] = adapter
		}
		if envList, err := parseEnvFlags(rawEnv); err != nil {
			return err
		} else if len(envList) > 0 {
			config["environments"] = envList
		}
		jsonBody, err := json.Marshal(config)
		if err != nil {
			return fmt.Errorf("failed to encode cluster spec: %w", err)
		}

		printClusterLaunchHeader(serviceName, image, httpPort, peerPort, numReplicas, stateDir, adapter)

		if err := sendCreateRequest(serviceName, string(jsonBody)); err != nil {
			return err
		}

		colorPrint := color.New(color.FgGreen).Add(color.Bold)
		_, _ = colorPrint.Printf("Cluster service launched 🎉\n")
		fmt.Printf("Inspect placement:   xdn service info %s\n", serviceName)
		fmt.Printf("Tear down:           xdn service destroy %s\n", serviceName)
		fmt.Println()
		return nil
	},
}

func init() {
	ClusterLaunchCmd.Flags().StringP("image", "i", "", "docker image for each cluster replica (required)")
	ClusterLaunchCmd.Flags().IntP("port", "p", 80, "HTTP entry port forwarded by XDN's frontend")
	ClusterLaunchCmd.Flags().Int("peer-port", 0, "cluster's internal peer/protocol port (required)")
	ClusterLaunchCmd.Flags().StringP("state", "s", "", "absolute state directory inside the container, ending with '/' (required)")
	ClusterLaunchCmd.Flags().IntP("num-replicas", "n", 0, "fixed cluster size — number of replicas (required)")
	ClusterLaunchCmd.Flags().String("adapter", "", "cluster lifecycle adapter (e.g. \"etcd\") for membership hooks")
	ClusterLaunchCmd.Flags().StringArrayP("env", "e", []string{}, "environment variables for the cluster image (repeat: --env KEY=VALUE)")
	_ = ClusterLaunchCmd.MarkFlagRequired("image")
	_ = ClusterLaunchCmd.MarkFlagRequired("peer-port")
	_ = ClusterLaunchCmd.MarkFlagRequired("state")
	_ = ClusterLaunchCmd.MarkFlagRequired("num-replicas")

	ClusterRootCmd.AddCommand(ClusterLaunchCmd)
}

func validateServiceName(name string) error {
	if name == "" {
		return errors.New("service name is required")
	}
	if len(name) > 64 {
		return errors.New("service name cannot exceed 64 characters")
	}
	alphanumeric := regexp.MustCompile(`^[a-zA-Z0-9\-_]+$`)
	if !alphanumeric.MatchString(name) {
		return errors.New("service name can only contain alphanumerics, '-', and '_'")
	}
	return nil
}

// parseEnvFlags converts the repeated --env KEY=VALUE flag values into the JSON shape that
// ServiceProperty.parseEnvironmentVariables expects (a list of single-entry maps).
func parseEnvFlags(raw []string) ([]map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]map[string]string, 0, len(raw))
	for _, kv := range raw {
		idx := strings.Index(kv, "=")
		if idx <= 0 {
			return nil, fmt.Errorf("invalid --env value %q (expected KEY=VALUE)", kv)
		}
		out = append(out, map[string]string{kv[:idx]: kv[idx+1:]})
	}
	return out, nil
}

func printClusterLaunchHeader(name, image string, httpPort, peerPort, n int, stateDir, adapter string) {
	colorPrint := color.New(color.FgYellow).Add(color.Bold).Add(color.Underline)
	fmt.Printf("Launching cluster ")
	_, _ = colorPrint.Printf("%s", name)
	fmt.Printf(" with the following configuration:\n")
	fmt.Printf(" docker image  : %s\n", image)
	fmt.Printf(" http port     : %d  (XDN frontend → local replica)\n", httpPort)
	fmt.Printf(" peer port     : %d  (cluster-internal; on overlay only)\n", peerPort)
	fmt.Printf(" state dir     : %s\n", stateDir)
	fmt.Printf(" num replicas  : %d\n", n)
	if adapter != "" {
		fmt.Printf(" adapter       : %s\n", adapter)
	}
	fmt.Println()
}
