package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/bigcommerce/russ/internal/docker"
	redisclient "github.com/bigcommerce/russ/internal/redis"
)

// printInstanceTable writes a status table for the given Redis instances,
// deriving live role from INFO REPLICATION. The masterPort parameter is unused
// in cluster mode (pass 0) but kept for call-site compatibility.
func printInstanceTable(ctx context.Context, instances []docker.ContainerInfo, _ int) {
	fmt.Printf("%-40s  %-9s  %-4s  %-6s  %-38s  %s\n",
		"NAME", "ROLE", "VER", "PORT", "REPLICATING FROM", "STATUS")
	fmt.Println(strings.Repeat("-", 115))

	for _, ci := range instances {
		role := "?"
		replicating := "-"

		rep, err := redisclient.GetReplicationState(ctx, ci.Port)
		if err != nil {
			role = "unreachable"
		} else {
			switch rep.Role {
			case "master":
				role = "master"
			case "slave":
				role = "replica"
				replicating = fmt.Sprintf("%s:%d (%s)", rep.MasterHost, rep.MasterPort, rep.MasterLinkStatus)
			default:
				role = rep.Role
			}
		}

		ver := fmt.Sprintf("v%d", ci.Version)
		fmt.Printf("%-40s  %-9s  %-4s  %-6d  %-38s  %s\n",
			ci.Name, role, ver, ci.Port, replicating, ci.Status)
	}
}
