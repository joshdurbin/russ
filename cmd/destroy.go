package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/bigcommerce/russ/internal/docker"
	"github.com/bigcommerce/russ/internal/state"
	"github.com/spf13/cobra"
)

var destroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "Destroy all russ-managed containers and the russ Docker network",
	Long: `Stops and removes every container that russ has created (sentinels and
Redis instances across all clusters), removes the "russ" Docker network, and
deletes all persisted upgrade state from ~/.russ/state/.

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

	// Collect cluster names before removing anything so we can clean up state files.
	clusterNames := map[string]struct{}{}
	for _, c := range containers {
		if c.ClusterName != "" {
			clusterNames[c.ClusterName] = struct{}{}
		}
	}

	for _, c := range containers {
		if err := dm.StopAndRemove(ctx, c.Name); err != nil {
			fmt.Fprintf(os.Stderr, "  warning: %v\n", err)
		} else {
			fmt.Printf("  ✓ removed %s\n", c.Name)
		}
	}

	// Remove the shared Docker network.
	if err := dm.RemoveNetwork(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "  warning: remove network %q: %v\n", docker.NetworkName, err)
	} else {
		fmt.Printf("  ✓ removed Docker network %q\n", docker.NetworkName)
	}

	// Clean up persisted upgrade state for every cluster we saw.
	for name := range clusterNames {
		if err := state.Delete(name); err != nil {
			fmt.Fprintf(os.Stderr, "  warning: delete state for %q: %v\n", name, err)
		} else {
			fmt.Printf("  ✓ deleted upgrade state for cluster %q\n", name)
		}
	}

	fmt.Println("\nAll russ resources destroyed.")
	return nil
}
