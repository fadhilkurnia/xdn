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
	"os"
	"regexp"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
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

		// File-based path: needed for multi-component clusters (e.g. bookcatalog + local
		// rqlite sidecar) since flags can only express a single image.
		if fileName, _ := cmd.Flags().GetString("file"); fileName != "" {
			return runClusterLaunchFromFile(serviceName, fileName)
		}

		image, _ := cmd.Flags().GetString("image")
		httpPort, _ := cmd.Flags().GetInt("port")
		peerPort, _ := cmd.Flags().GetInt("peer-port")
		stateDir, _ := cmd.Flags().GetString("state")
		numReplicas, _ := cmd.Flags().GetInt("num-replicas")
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
		if envList, err := parseEnvFlags(rawEnv); err != nil {
			return err
		} else if len(envList) > 0 {
			config["environments"] = envList
		}
		jsonBody, err := json.Marshal(config)
		if err != nil {
			return fmt.Errorf("failed to encode cluster spec: %w", err)
		}

		printClusterLaunchHeader(serviceName, image, httpPort, peerPort, numReplicas, stateDir)

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
	ClusterLaunchCmd.Flags().StringArrayP("env", "e", []string{}, "environment variables for the cluster image (repeat: --env KEY=VALUE)")
	ClusterLaunchCmd.Flags().StringP("file", "f", "", "YAML/JSON file with the cluster spec (required for multi-component clusters)")
	// Flag-based path requires these; file-based path validates them inside the YAML instead.
	// Bypass the cobra requireds when -f is used by un-marking after the fact in PreRunE.
	ClusterLaunchCmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if fileName, _ := cmd.Flags().GetString("file"); fileName != "" {
			return nil
		}
		for _, f := range []string{"image", "peer-port", "state", "num-replicas"} {
			if !cmd.Flags().Changed(f) {
				return fmt.Errorf("--%s is required (or pass --file <spec.yaml>)", f)
			}
		}
		return nil
	}

	ClusterRootCmd.AddCommand(ClusterLaunchCmd)
}

// runClusterLaunchFromFile reads a YAML/JSON cluster spec from disk and hands it to the
// control plane. The file is the only way to declare a multi-component cluster (one stateful
// member + one or more sidecars sharing its network namespace).
func runClusterLaunchFromFile(serviceName, fileName string) error {
	body, err := os.ReadFile(fileName)
	if err != nil {
		return fmt.Errorf("unable to read spec file %s: %w", fileName, err)
	}
	jsonBody, err := yaml.YAMLToJSON(body)
	if err != nil {
		return fmt.Errorf("unable to parse spec file %s: %w", fileName, err)
	}
	var spec map[string]interface{}
	if err := json.Unmarshal(jsonBody, &spec); err != nil {
		return fmt.Errorf("spec file is not valid JSON/YAML: %w", err)
	}
	specName, _ := spec["name"].(string)
	if specName != "" && specName != serviceName {
		return fmt.Errorf(
			"service name on command line (%q) doesn't match spec file 'name' (%q)",
			serviceName, specName)
	}
	if specName == "" {
		// Default the name from the command line if the YAML omits it, so the user can reuse
		// the same spec under different names.
		spec["name"] = serviceName
		jsonBody, _ = json.Marshal(spec)
	}
	// Sanity-check mode: we won't refuse a non-cluster spec here (the validator will), but a
	// warning helps catch accidentally passing a regular-launch YAML to `xdn cluster launch`.
	if m, _ := spec["mode"].(string); m != "cluster" && m != "clustered" {
		colorPrint := color.New(color.FgYellow)
		_, _ = colorPrint.Fprintf(os.Stderr,
			"warning: spec file does not set mode: cluster; the control plane will reject it\n")
	}

	printClusterLaunchFileHeader(serviceName, fileName, spec)

	if err := sendCreateRequest(serviceName, string(jsonBody)); err != nil {
		return err
	}
	colorPrint := color.New(color.FgGreen).Add(color.Bold)
	_, _ = colorPrint.Printf("Cluster service launched 🎉\n")
	fmt.Printf("Inspect placement:   xdn service info %s\n", serviceName)
	fmt.Printf("Tear down:           xdn service destroy %s\n", serviceName)
	fmt.Println()
	return nil
}

func printClusterLaunchFileHeader(name, fileName string, spec map[string]interface{}) {
	colorPrint := color.New(color.FgYellow).Add(color.Bold).Add(color.Underline)
	fmt.Printf("Launching cluster ")
	_, _ = colorPrint.Printf("%s", name)
	fmt.Printf(" from spec %s with the following configuration:\n", fileName)
	if v, ok := spec["peer_port"]; ok {
		fmt.Printf(" peer port     : %v\n", v)
	}
	if v, ok := spec["num_replicas"]; ok {
		fmt.Printf(" num replicas  : %v\n", v)
	}
	if v, ok := spec["state"]; ok {
		fmt.Printf(" state dir     : %v\n", v)
	}
	if comps, ok := spec["components"].([]interface{}); ok {
		fmt.Printf(" components    :\n")
		for _, c := range comps {
			if m, ok := c.(map[string]interface{}); ok {
				for cname, body := range m {
					if bm, ok := body.(map[string]interface{}); ok {
						img, _ := bm["image"].(string)
						role := ""
						if v, _ := bm["stateful"].(bool); v {
							role += " stateful"
						}
						if v, _ := bm["entry"].(bool); v {
							role += " entry"
						}
						fmt.Printf("   - %-12s %s%s\n", cname+":", img, role)
					}
				}
			}
		}
	} else if v, ok := spec["image"]; ok {
		fmt.Printf(" docker image  : %v\n", v)
	}
	fmt.Println()
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

func printClusterLaunchHeader(name, image string, httpPort, peerPort, n int, stateDir string) {
	colorPrint := color.New(color.FgYellow).Add(color.Bold).Add(color.Underline)
	fmt.Printf("Launching cluster ")
	_, _ = colorPrint.Printf("%s", name)
	fmt.Printf(" with the following configuration:\n")
	fmt.Printf(" docker image  : %s\n", image)
	fmt.Printf(" http port     : %d  (XDN frontend → local replica)\n", httpPort)
	fmt.Printf(" peer port     : %d  (cluster-internal; on overlay only)\n", peerPort)
	fmt.Printf(" state dir     : %s\n", stateDir)
	fmt.Printf(" num replicas  : %d\n", n)
	fmt.Println()
}
