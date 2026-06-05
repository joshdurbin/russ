package cmd

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var quickstartCmd = &cobra.Command{
	Use:   "quickstart",
	Short: "One-shot demo: destroy existing resources, bring up a Redis 8 Cluster + client workload",
	Long: `Single-shot setup that:

  1. Destroys every russ-managed container (no prompt)
  2. Starts the observability stack (Prometheus + redis-exporter + Grafana)
  3. Creates a 6-node Redis 8 Cluster (3 masters + 3 replicas)
  4. Builds the russ-client image if needed
  5. Starts the orders analytics workload against the cluster

After quickstart, the cluster is live under continuous write load.`,
	RunE: runQuickstart,
}

func init() {
	quickstartCmd.Flags().String("cluster-name", "quickstart-1", "Name for the cluster to create")
	quickstartCmd.Flags().Int("masters", 3, "Number of master nodes")
	quickstartCmd.Flags().Int("replicas-per-master", 1, "Replicas per master node")
	rootCmd.AddCommand(quickstartCmd)
}

func runQuickstart(cmd *cobra.Command, _ []string) error {
	clusterName, _ := cmd.Flags().GetString("cluster-name")
	masters, _ := cmd.Flags().GetInt("masters")
	replicasPerMaster, _ := cmd.Flags().GetInt("replicas-per-master")

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
			fmt.Sprintf("create Redis 8 Cluster %q (%d masters × %d replicas)", clusterName, masters, replicasPerMaster),
			func() error {
				_ = clusterCreateCmd.Flags().Set("masters", fmt.Sprintf("%d", masters))
				_ = clusterCreateCmd.Flags().Set("replicas-per-master", fmt.Sprintf("%d", replicasPerMaster))
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
			fmt.Sprintf("start russ-client (writer + processor + api) for %q", clusterName),
			func() error {
				_ = clientWorkloadStartCmd.Flags().Set("min-clients", "1")
				_ = clientWorkloadStartCmd.Flags().Set("max-clients", "4")
				return clientWorkloadStartCmd.RunE(clientWorkloadStartCmd, []string{clusterName})
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
	fmt.Printf("  Cluster:       %s (%d masters + %d replicas)\n", clusterName, masters, masters*replicasPerMaster)
	fmt.Println("  Prometheus UI: http://127.0.0.1:9090")
	fmt.Println("  Grafana UI:    http://127.0.0.1:3000")
	fmt.Printf("  Client logs:   docker logs -f russ-client-%s\n", clusterName)
	fmt.Println("  REST API:      http://127.0.0.1:9300/api/analytics/summary")
	fmt.Println()
	return nil
}
