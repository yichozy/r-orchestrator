//go:build e2e

package e2e

import (
	"os"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestClusterProvisioning(t *testing.T) {
	requireK8s(t)

	tenantName := "e2e-cluster-provision"
	tenantID := createTestTenant(t, tenantName, 1)
	defer cleanupTestTenant(t, tenantID)

	taskID := submitTestTask(t, tenantName,
		"test-data/e2e-bundle/bundle.zip",
	)

	// Wait for poll_pending_tasks to provision the cluster.
	stsName := clusterName(tenantID.String())
	waitForStatefulSet(t, stsName, "created", 2*time.Minute)

	// Wait for task to complete or timeout.
	waitForTaskStatus(t, taskID, "SUCCEEDED", 10*time.Minute)
}

func TestClusterIdleRecycling(t *testing.T) {
	requireK8s(t)

	tenantName := "e2e-cluster-recycle"
	tenantID := createTestTenant(t, tenantName, 1)
	defer cleanupTestTenant(t, tenantID)

	taskID := submitTestTask(t, tenantName,
		"test-data/e2e-bundle/bundle.zip",
	)

	stsName := clusterName(tenantID.String())
	waitForStatefulSet(t, stsName, "created", 2*time.Minute)

	waitForTaskStatus(t, taskID, "SUCCEEDED", 10*time.Minute)

	// Wait for idle recycling. The idle threshold is configurable;
	// use a short timeout for the test.
	waitForStatefulSet(t, stsName, "deleted", 5*time.Minute)
}

// clusterName generates the StatefulSet name for a tenant, matching the
// naming convention in k8s_backend/provider.go.
func clusterName(tenantID string) string {
	return "r-orch-" + tenantID[:8] + "-agents"
}

func getK8sClient(t *testing.T) *kubernetes.Clientset {
	t.Helper()
	kubeconfigPath := os.Getenv("CLUSTER_KUBERNETES_KUBECONFIG_PATH")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		t.Fatalf("build kube config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("create k8s client: %v", err)
	}
	return clientset
}

func waitForStatefulSet(t *testing.T, name, state string, timeout time.Duration) {
	t.Helper()
	clientset := getK8sClient(t)
	namespace := os.Getenv("CLUSTER_KUBERNETES_NAMESPACE")
	if namespace == "" {
		namespace = "r-agents"
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := clientset.AppsV1().StatefulSets(namespace).Get(testCtx, name, metav1.GetOptions{})
		if state == "created" && err == nil {
			return
		}
		if state == "deleted" && errors.IsNotFound(err) {
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("timed out waiting for StatefulSet %s to be %s", name, state)
}
