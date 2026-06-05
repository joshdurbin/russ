package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/bigcommerce/russ/internal/docker"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var destroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "Destroy all russ-managed containers and the russ Docker network",
	Long: `Stops and removes every container that russ has created (Redis instances,
client containers, and observability stack), removes the "russ" Docker network.

Prompts for confirmation unless --yes is supplied.`,
	RunE: runDestroy,
}

func init() {
	destroyCmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	rootCmd.AddCommand(destroyCmd)
}

func runDestroy(cmd *cobra.Command, _ []string) error {
	yes, _ := cmd.Flags().GetBool("yes")

	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	containers, err := dm.ListContainers(ctx)
	if err != nil {
		return err
	}

	if len(containers) == 0 {
		fmt.Println("No russ-managed containers found.")
		return nil
	}

	fmt.Printf("This will destroy %d container(s):\n", len(containers))
	for _, c := range containers {
		var parts []string
		if c.Version > 0 {
			parts = append(parts, fmt.Sprintf("v%d", c.Version), fmt.Sprintf("port=%d", c.Port))
		}
		if c.ClusterName != "" {
			parts = append(parts, fmt.Sprintf("cluster=%s", c.ClusterName))
		}
		fmt.Printf("  %-40s  %-16s  %s\n", c.Name, c.Role, strings.Join(parts, "  "))
	}
	fmt.Println()

	if !yes {
		fmt.Print("Destroy all of the above? [y/N] ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
		if answer != "y" && answer != "yes" {
			fmt.Println("Aborted.")
			return nil
		}
	}

	for _, c := range containers {
		if err := dm.StopAndRemove(ctx, c.Name); err != nil {
			log.Warn().Err(err).Str("name", c.Name).Msg("remove container")
		} else {
			log.Info().Str("name", c.Name).Msg("removed container")
		}
	}

	// Sweep any russ-managed Docker volumes that survived the container
	// teardown (StopAndRemove already removes the volume backing each Redis
	// instance it tears down, so this typically only catches orphans from a
	// previously-aborted run or volumes whose owning container was deleted
	// manually outside russ).
	volumes, _ := dm.ListManagedVolumes(ctx)
	for _, name := range volumes {
		if err := dm.RemoveVolume(ctx, name, true); err != nil {
			log.Warn().Err(err).Str("volume", name).Msg("remove volume")
		} else {
			log.Info().Str("volume", name).Msg("removed volume")
		}
	}

	// Remove the shared Docker network.
	if err := dm.RemoveNetwork(ctx); err != nil {
		log.Warn().Err(err).Str("network", docker.NetworkName).Msg("remove network")
	} else {
		log.Info().Str("network", docker.NetworkName).Msg("removed network")
	}

	log.Info().Msg("all russ resources destroyed")
	return nil
}
