package k8

import (
	"context"
	"fmt"

	"github.com/yichozy/r-orchestrator/internal/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DestroyCluster 销毁指定租户的集群
func (p *K8sProvider) DestroyCluster(ctx context.Context, tenant model.Tenant) error {
	name := clusterName(tenant.ID)

	err := p.client.AppsV1().StatefulSets(p.cfg.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete statefulset %s: %w", name, err)
	}

	return nil
}
