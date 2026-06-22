package k8

import (
	"context"
	"fmt"

	"github.com/yichozy/r-orchestrator/internal/model"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ProvisionCluster 确保指定租户的 K8s 资源存在，若不存在则创建
func (p *K8sProvider) ProvisionCluster(ctx context.Context, tenant model.Tenant) error {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: p.cfg.Namespace,
		},
	}
	_, err := p.client.CoreV1().Namespaces().Get(ctx, p.cfg.Namespace, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get namespace %s: %w", p.cfg.Namespace, err)
		}
		_, err = p.client.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create namespace %s: %w", p.cfg.Namespace, err)
		}
	}

	name := clusterName(tenant.ID)
	replicas := int32(tenant.MaxAgents)
	if replicas < 1 {
		replicas = 1
	}

	envs := []corev1.EnvVar{
		{Name: "RORCHESTRATOR_TENANT_ID", Value: tenant.ID.String()},
		{Name: "RORCHESTRATOR_SERVER_GRPC_ADDR", Value: p.cfg.ServerGRPCAddr},
		{Name: "RORCHESTRATOR_AGENT_TOKEN", Value: p.cfg.AgentToken},
		{Name: "RORCHESTRATOR_BACKEND_NAME", Value: "kubernetes"},
		{Name: "RORCHESTRATOR_HEALTH_PORT", Value: "9091"},
		{Name: "RUST_LOG", Value: p.cfg.AgentLogLevel},
		{Name: "ALIYUN_OSS_ENDPOINT", Value: p.cfg.OSS.Endpoint},
		{Name: "ALIYUN_OSS_BUCKET", Value: p.cfg.OSS.Bucket},
		{Name: "ALIYUN_OSS_ACCESS_KEY", Value: p.cfg.OSS.AccessKey},
		{Name: "ALIYUN_OSS_ACCESS_SECRET", Value: p.cfg.OSS.AccessSecret},
	}
	if p.cfg.ServerPublicURL != "" {
		envs = append(envs, corev1.EnvVar{
			Name:  "RORCHESTRATOR_SERVER_URL",
			Value: p.cfg.ServerPublicURL,
		})
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: p.cfg.Namespace,
			Labels: map[string]string{
				labelApp:    "true",
				labelTenant: tenant.ID.String(),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					labelApp:    "true",
					labelTenant: tenant.ID.String(),
				},
			},
			ServiceName: name,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						labelApp:    "true",
						labelTenant: tenant.ID.String(),
					},
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: func() []corev1.LocalObjectReference {
						refs := make([]corev1.LocalObjectReference, len(p.cfg.ImagePullSecrets))
						for i, name := range p.cfg.ImagePullSecrets {
							refs[i] = corev1.LocalObjectReference{Name: name}
						}
						return refs
					}(),
					Containers: []corev1.Container{
						{
							Name:            "agent",
							Image:           p.cfg.AgentImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env:             envs,
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromInt(9091),
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
				},
			},
		},
	}

	_, err = p.client.AppsV1().StatefulSets(p.cfg.Namespace).Get(ctx, sts.Name, metav1.GetOptions{})
	if err == nil {
		return nil
	}

	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get statefulset %s: %w", sts.Name, err)
	}

	_, err = p.client.AppsV1().StatefulSets(p.cfg.Namespace).Create(ctx, sts, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create statefulset %s: %w", sts.Name, err)
	}

	return nil
}
