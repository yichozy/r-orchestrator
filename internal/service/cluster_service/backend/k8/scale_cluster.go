package k8

import (
	"context"
	"fmt"

	"github.com/yichozy/r-orchestrator/internal/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ScaleCluster 调整指定租户集群的副本数
func (p *K8sProvider) ScaleCluster(ctx context.Context, tenant model.Tenant, replicas int) error {
	name := clusterName(tenant.ID)

	patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	_, err := p.client.AppsV1().StatefulSets(p.cfg.Namespace).Patch(
		ctx, name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("scale statefulset %s to %d: %w", name, replicas, err)
	}

	return nil
}
