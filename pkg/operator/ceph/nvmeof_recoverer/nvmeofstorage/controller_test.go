package nvmeofstorage

import (
	"context"
	"os"
	"testing"

	"github.com/coreos/pkg/capnslog"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	rookclientsetfake "github.com/rook/rook/pkg/client/clientset/versioned/fake"
	"github.com/rook/rook/pkg/clusterd"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/tools/record"
	// "github.com/stretchr/testify/mock"
	// "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestUpdateCephClusterCR(t *testing.T) {
	capnslog.SetGlobalLogLevel(capnslog.DEBUG)
	os.Setenv("ROOK_LOG_LEVEL", "DEBUG")

	// Set up the scheme for the fake client
	scheme := runtime.NewScheme()
	cephv1.AddToScheme(scheme)

	// Create a mock CephCluster
	clusterName := "test-ceph-cluster"
	namespace := "test-nvmeof-recoverer"

	cephCluster := &cephv1.CephCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: cephv1.ClusterSpec{
			Storage: cephv1.StorageScopeSpec{
				Nodes: []cephv1.Node{
					{
						Name: "node1",
						Config: map[string]string{
							"test_config": "valid_data",
						},
					},
				},
			},
		},
	}

	// Set up the fake Rook clientset with the CephCluster
	rookClientset := rookclientsetfake.NewSimpleClientset(cephCluster)

	// Set up the fake Kubernetes client with the CephCluster
	k8sClient := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(cephCluster).
		Build()

	// Set up the ReconcileNvmeOfStorage object
	clusterdContext := &clusterd.Context{
		RookClientset: rookClientset,
	}

	r := &ReconcileNvmeOfStorage{
		client:           k8sClient,
		scheme:           scheme,
		context:          clusterdContext,
		opManagerContext: context.TODO(),
		recorder:         &record.FakeRecorder{},
		nvmeOfStorage: &cephv1.NvmeOfStorage{
			Spec: cephv1.NvmeOfStorageSpec{
				Name: "nvme-storage",
				Devices: []cephv1.FabricDevice{
					{
						DeviceName:   "/dev/nvme0n1",
						AttachedNode: "node1",
					},
					{
						DeviceName:   "/dev/nvme1n1",
						AttachedNode: "node1",
					},
					{
						DeviceName:   "/dev/nvme0n1",
						AttachedNode: "node2",
					},
				},
			},
		},
	}

	// Prepare the reconcile request
	request := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
		},
	}

	// Call the function under test
	err := r.updateCephClusterCR(request, clusterName)
	assert.NoError(t, err)

	// Get the updated CephCluster from the fake Rook clientset
	updatedCephCluster, err := rookClientset.CephV1().CephClusters(namespace).Get(context.TODO(), clusterName, metav1.GetOptions{})
	assert.NoError(t, err)

	// Define the expected nodes
	expectedNodes := []cephv1.Node{
		{
			Name: "node1",
			Config: map[string]string{
				"test_config": "host",
			},
			Selection: cephv1.Selection{
				Devices: []cephv1.Device{
					{
						Name: "/dev/nvme0n1",
						Config: map[string]string{
							"failureDomain": FabricFailureDomainPrefix + "-" + r.nvmeOfStorage.Spec.Name,
						},
					},
					{
						Name: "/dev/nvme1n1",
						Config: map[string]string{
							"failureDomain": FabricFailureDomainPrefix + "-" + r.nvmeOfStorage.Spec.Name,
						},
					},
				},
			},
		},
		{
			Name: "node2",
			Selection: cephv1.Selection{
				Devices: []cephv1.Device{
					{
						Name: "/dev/nvme0n1",
						Config: map[string]string{
							"failureDomain": FabricFailureDomainPrefix + "-" + r.nvmeOfStorage.Spec.Name,
						},
					},
				},
			},
		},
	}

	// Verify the updated nodes
	assert.Equal(t, expectedNodes, updatedCephCluster.Spec.Storage.Nodes)
}
