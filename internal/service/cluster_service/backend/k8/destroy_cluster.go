package k8

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DestroyCluster deletes the StatefulSet for the given tenant.
// If the StatefulSet is already gone, this is a no-op.
func (p *K8sProvider) DestroyCluster(ctx context.Context, tenant model.Tenant) error {
	name := clusterName(tenant.ID)

	err := p.client.AppsV1().StatefulSets(p.cfg.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}
