package cmd

import (
	"fmt"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var quickstartCmd = &cobra.Command{
	Use:   "quickstart",
	Short: "One-shot demo setup: destroy, then bring up obs + v6 sentinels + a mixed v6/v8 cluster + a running client",
	Long: `Single-shot setup that:

  1. Destroys every russ-managed container (no prompt)
  2. Starts the observability stack (Prometheus + shared redis-exporter + Grafana)
  3. Adds 3 v6 sentinels (initial fleet)
  4. Creates a v6 cluster with 3 instances
  5. Builds the russ-client image if needed
  6. Starts the client workload against the v6-only cluster
  7. Adds 3 v8 instances to the (already-loaded) cluster

The workload starts before v8 instances are added so the v8 replicas have to
catch up against a master that's already under sustained write load — the
realistic production scenario when you're staging an upgrade.

The cluster ends up in lifecycle state "MixedVersions" with 3 v6 + 3 v8
instances and a live client. The v8 instances enter with a deprioritized
replica-priority (200) so any operational failover triggered from this
state stays within the v6 subset and replication is preserved. Run
'russ cluster promote-v8 quickstart-1' to commit to the upgrade, then
'russ cluster failover quickstart-1' to actually promote a v8 master.`,
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

	log.Warn().Msg("quickstart will destroy every russ-managed container before setting up a fresh demo environment")

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
			"add 3 v6 sentinels (initial fleet)",
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
			"build russ-client image (skipped if cached)",
			func() error {
				buildCmd := newClientBuildCmd()
				return buildCmd.RunE(buildCmd, nil)
			},
		},
		{
			fmt.Sprintf("start russ-client (writer + processor + api) for %q against v6-only cluster", clusterName),
			func() error {
				// Quickstart caps the writer's wave low so person:insert
				// task volume stays modest while v8 replicas are still
				// being added. After quickstart, stop + restart with higher
				// --max-clients to drive heavier traffic against the now-
				// stable mixed-version cluster.
				_ = clientWorkloadStartCmd.Flags().Set("min-clients", "1")
				_ = clientWorkloadStartCmd.Flags().Set("max-clients", "4")
				_ = clientWorkloadStartCmd.Flags().Set("wave-period", "5m")
				return clientWorkloadStartCmd.RunE(clientWorkloadStartCmd, []string{clusterName})
			},
		},
		{
			fmt.Sprintf("add %d v8 instances to %q (workload already running)", v8Count, clusterName),
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
	}

	for i, s := range steps {
		log.Info().Int("step", i+1).Int("total", len(steps)).Str("label", s.label).Msg("starting step")
		if err := s.run(); err != nil {
			log.Error().Err(err).Int("step", i+1).Str("label", s.label).Msg("quickstart aborted")
			return err
		}
	}

	log.Info().Msg("quickstart complete")
	fmt.Println()
	fmt.Println("  Cluster:           " + clusterName + " (3 v6 + " + strconv.Itoa(v8Count) + " v8, state MixedVersions)")
	fmt.Println("  Prometheus UI:     http://127.0.0.1:9090")
	fmt.Println("  Grafana UI:        http://127.0.0.1:3000  (anonymous admin; redis dashboard preloaded)")
	fmt.Println("  Cluster state:     russ cluster status " + clusterName)
	fmt.Println("  Upgrade FSM tree:  russ cluster lifecycle " + clusterName)
	fmt.Println("  Client logs:       docker logs -f russ-client-" + clusterName)
	fmt.Println("  REST API:          http://127.0.0.1:9300/api/analytics/summary")
	fmt.Println()
	fmt.Println("  Next upgrade step: russ cluster promote-v8 " + clusterName)
	return nil
}
