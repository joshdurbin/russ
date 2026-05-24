package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/bigcommerce/russ/internal/docker"
	redisclient "github.com/bigcommerce/russ/internal/redis"
)

// printInstanceTable writes a status table for the given Redis instances,
// deriving live role and replication target from each node's INFO replication
// rather than from the container's static role label. The role column reflects
// reality after failovers; the "REPLICATING FROM" column makes broken
// replication (REPLICAOF NO ONE, link down, accidental chains) visible.
//
// sentinelMasterPort is the port sentinel currently reports as the master, or
// 0 if no sentinel could be queried. It's used to flag nodes that locally
// report role=master but aren't the cluster leader (i.e., have been isolated).
func printInstanceTable(ctx context.Context, instances []docker.ContainerInfo, sentinelMasterPort int) {
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
				if sentinelMasterPort > 0 && ci.Port != sentinelMasterPort {
					// Node thinks it's a master but sentinel says otherwise —
					// typically a REPLICAOF NO ONE or a stale promotion.
					role = "isolated"
				} else {
					role = "master"
				}
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
