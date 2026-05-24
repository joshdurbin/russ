package docker

import (
	"context"
	"fmt"
	"strings"

	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/volume"
)

// LabelDataVolume names the Docker volume attached to a Redis container as
// its persistent data dir. Stored on the *container* so StopAndRemove can
// look it up before tearing down the container.
const LabelDataVolume = "russ.data.volume"

// RedisDataVolumeName is the canonical Docker volume name for a Redis
// instance's persistent data (AOF + RDB). Name pattern matches the container
// name pattern (russ-<cluster>-<port>) so the relationship is grep-obvious.
func RedisDataVolumeName(clusterName string, port int) string {
	return fmt.Sprintf("russ-data-%s-%d", clusterName, port)
}

// EnsureVolume creates a russ-managed Docker volume if one with this name
// doesn't already exist. Volumes are labeled russ.managed=true (and tagged
// with the owning cluster) so they're discoverable for orphan cleanup.
func (m *Manager) EnsureVolume(ctx context.Context, name, clusterName string) error {
	list, err := m.cli.VolumeList(ctx, volume.ListOptions{
		Filters: filters.NewArgs(filters.Arg("name", name)),
	})
	if err != nil {
		return fmt.Errorf("list volumes: %w", err)
	}
	for _, v := range list.Volumes {
		if v.Name == name {
			return nil
		}
	}

	labels := map[string]string{LabelManaged: "true"}
	if clusterName != "" {
		labels[LabelCluster] = clusterName
	}
	if _, err := m.cli.VolumeCreate(ctx, volume.CreateOptions{
		Name:   name,
		Labels: labels,
	}); err != nil {
		return fmt.Errorf("create volume %s: %w", name, err)
	}
	return nil
}

// RemoveVolume removes a Docker volume. force=true wins against "in use" if
// the container is still attached (shouldn't happen since we remove the
// container first, but defensive). Not-found errors are ignored.
func (m *Manager) RemoveVolume(ctx context.Context, name string, force bool) error {
	err := m.cli.VolumeRemove(ctx, name, force)
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "no such volume") || strings.Contains(msg, "not found") {
		return nil
	}
	return fmt.Errorf("remove volume %s: %w", name, err)
}

// ListManagedVolumes returns every russ-managed Docker volume name.
func (m *Manager) ListManagedVolumes(ctx context.Context) ([]string, error) {
	list, err := m.cli.VolumeList(ctx, volume.ListOptions{
		Filters: filters.NewArgs(filters.Arg("label", LabelManaged+"=true")),
	})
	if err != nil {
		return nil, fmt.Errorf("list volumes: %w", err)
	}
	names := make([]string, 0, len(list.Volumes))
	for _, v := range list.Volumes {
		names = append(names, v.Name)
	}
	return names, nil
}
