package cmd

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

var quickstartCmd = &cobra.Command{
	Use:   "quickstart",
	Short: "One-shot demo setup: destroy, then bring up obs + v6 sentinels + a mixed v6/v8 cluster + a running client",
	Long: `Single-shot setup that:

  1. Destroys every russ-managed container (no prompt)
  2. Starts the observability stack (Prometheus + shared redis-exporter)
  3. Bootstraps 3 v6 sentinels
  4. Creates a v6 cluster with 3 instances
  5. Adds 3 v8 instances to the same cluster
  6. Builds the russ-client image if needed
  7. Starts the client workload (writer + master reader + replica observer) for the cluster

The cluster ends up in lifecycle state "MixedVersions" with 3 v6 + 3 v8 instances
and a live client driving health-check traffic. Useful for grabbing a fresh
demo environment in one command, or as a known-good starting point before
walking the upgrade FSM by hand.`,
	RunE: runQuickstart,
}

func init() {
	quickstartCmd.Flags().String("cluster-name", "quickstart-1", "Name for the cluster to create")
	quickstartCmd.Flags().Int("v8-count", 3, "Number of v8 instances to add to the cluster after creation")
	rootCmd.AddCommand(quickstartCmd)
}

func runQuickstart(cmd *cobra.Command, _ []string) error {
	clusterName, _ := cmd.Flags().GetString("cluster-name")
	v8Count, _ := cmd.Flags().GetInt("v8-count")

	fmt.Println("⚠  Quickstart will destroy every russ-managed container before setting up a fresh demo environment.")
	fmt.Println()

	type step struct {
		label string
		run   func() error
	}
	steps := []step{
		{
			"destroy existing containers",
			func() error {
				_ = destroyCmd.Flags().Set("yes", "true")
				return destroyCmd.RunE(destroyCmd, nil)
			},
		},
		{
			"start observability stack",
			func() error {
				return obsStartCmd.RunE(obsStartCmd, nil)
			},
		},
		{
			"bootstrap 3 v6 sentinels",
			func() error {
				_ = sentinelBootstrapCmd.Flags().Set("version", "6")
				_ = sentinelBootstrapCmd.Flags().Set("count", "3")
				return sentinelBootstrapCmd.RunE(sentinelBootstrapCmd, nil)
			},
		},
		{
			fmt.Sprintf("create v6 cluster %q (3 instances)", clusterName),
			func() error {
				_ = clusterCreateCmd.Flags().Set("version", "6")
				_ = clusterCreateCmd.Flags().Set("count", "3")
				return clusterCreateCmd.RunE(clusterCreateCmd, []string{clusterName})
			},
		},
		{
			fmt.Sprintf("add %d v8 instances to %q", v8Count, clusterName),
			func() error {
				_ = instanceAddCmd.Flags().Set("version", "8")
				for i := 0; i < v8Count; i++ {
					if err := instanceAddCmd.RunE(instanceAddCmd, []string{clusterName}); err != nil {
						return fmt.Errorf("instance %d/%d: %w", i+1, v8Count, err)
					}
				}
				return nil
			},
		},
		{
			"build russ-client image (skipped if cached)",
			func() error {
				buildCmd := newClientBuildCmd()
				return buildCmd.RunE(buildCmd, nil)
			},
		},
		{
			fmt.Sprintf("start client workload for %q", clusterName),
			func() error {
				return clientWorkloadStartCmd.RunE(clientWorkloadStartCmd, []string{clusterName})
			},
		},
	}

	for i, s := range steps {
		fmt.Printf("\n=== Step %d/%d: %s ===\n", i+1, len(steps), s.label)
		if err := s.run(); err != nil {
			fmt.Fprintf(os.Stderr, "\nquickstart aborted at step %d (%s): %v\n", i+1, s.label, err)
			return err
		}
	}

	fmt.Println()
	fmt.Println("✓ Quickstart complete.")
	fmt.Println()
	fmt.Println("  Cluster:           " + clusterName + " (3 v6 + " + strconv.Itoa(v8Count) + " v8, state MixedVersions)")
	fmt.Println("  Prometheus UI:     http://127.0.0.1:9090")
	fmt.Println("  Cluster state:     russ cluster status " + clusterName)
	fmt.Println("  Upgrade FSM tree:  russ cluster lifecycle " + clusterName)
	fmt.Println("  Client logs:       docker logs -f russ-workload-" + clusterName)
	fmt.Println()
	fmt.Println("  Next upgrade step: russ sentinel failover " + clusterName)
	return nil
}
