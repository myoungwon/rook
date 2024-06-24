/*
Copyright 2024 The Rook Authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package nvmeofstorage to reconcile a NvmeOfStorage CR.
package nvmeofstorage

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"emperror.dev/errors"
	"github.com/coreos/pkg/capnslog"

	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	"github.com/rook/rook/pkg/operator/ceph/cluster/osd"
	opcontroller "github.com/rook/rook/pkg/operator/ceph/controller"
	cm "github.com/rook/rook/pkg/operator/ceph/nvmeof_recoverer/clustermanager"
	"github.com/rook/rook/pkg/operator/ceph/reporting"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	controllerName = "nvmeofstorage-controller"
)

var logger = capnslog.NewPackageLogger("github.com/rook/rook", controllerName)

var nvmeOfStorageKind = reflect.TypeOf(cephv1.NvmeOfStorage{}).Name()

// Sets the type meta for the controller main object
var controllerTypeMeta = metav1.TypeMeta{
	Kind:       nvmeOfStorageKind,
	APIVersion: fmt.Sprintf("%s/%s", cephv1.CustomResourceGroup, cephv1.Version),
}

var _ reconcile.Reconciler = &ReconcileNvmeOfStorage{}

// ReconcileNvmeOfStorage reconciles a NvmeOfStorage object
type ReconcileNvmeOfStorage struct {
	client           client.Client
	scheme           *runtime.Scheme
	context          *clusterd.Context
	opManagerContext context.Context
	recorder         record.EventRecorder
	clustermanager   *cm.ClusterManager
	nvmeOfStorage    *cephv1.NvmeOfStorage
}

// Add creates a new NvmeOfStorage Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, context *clusterd.Context, opManagerContext context.Context, opConfig opcontroller.OperatorConfig) error {
	return add(mgr, newReconciler(mgr, context, opManagerContext))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, context *clusterd.Context, opManagerContext context.Context) reconcile.Reconciler {
	return &ReconcileNvmeOfStorage{
		client:           mgr.GetClient(),
		context:          context,
		scheme:           mgr.GetScheme(),
		opManagerContext: opManagerContext,
		recorder:         mgr.GetEventRecorderFor("rook-" + controllerName),
		nvmeOfStorage:    &cephv1.NvmeOfStorage{},
		clustermanager:   cm.New(),
	}
}

func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New(controllerName, mgr, controller.Options{Reconciler: r})
	if err != nil {
		return errors.Wrapf(err, "failed to create %s controller", controllerName)
	}
	logger.Info("successfully started")

	// Watch for changes on the NvmeOfStorage CRD object
	cmKind := source.Kind(
		mgr.GetCache(),
		&cephv1.NvmeOfStorage{TypeMeta: controllerTypeMeta})
	err = c.Watch(cmKind, &handler.EnqueueRequestForObject{}, opcontroller.WatchControllerPredicate())
	if err != nil {
		return err
	}

	// Watch for changes on the OSD Pod object
	podKind := source.Kind(
		mgr.GetCache(),
		&corev1.Pod{})
	err = c.Watch(podKind, &handler.EnqueueRequestForObject{},
		predicate.Funcs{
			UpdateFunc: func(event event.UpdateEvent) bool {
				oldPod, okOld := event.ObjectOld.(*corev1.Pod)
				newPod, okNew := event.ObjectNew.(*corev1.Pod)
				if !okOld || !okNew {
					return false
				}
				if isOSDPod(newPod.Labels) && isPodDead(oldPod, newPod) {
					namespacedName := fmt.Sprintf("%s/%s", newPod.Namespace, newPod.Name)
					logger.Debugf("update event on Pod %q", namespacedName)
					return true
				}
				return false
			},
			CreateFunc: func(e event.CreateEvent) bool {
				return false
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				return false
			},
			GenericFunc: func(e event.GenericEvent) bool {
				return false
			},
		})
	if err != nil {
		return errors.Wrap(err, "failed to watch for changes on the Pod object")
	}

	return nil
}

func (r *ReconcileNvmeOfStorage) Reconcile(context context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger.Debugf("reconciling NvmeOfStorage. Request.Namespace: %s, Request.Name: %s", request.Namespace, request.Name)

	if strings.Contains(request.Name, "nvmeofstorage") {
		// Fetch the NvmeOfStorage CRD object
		err := r.client.Get(r.opManagerContext, request.NamespacedName, r.nvmeOfStorage)
		if err != nil {
			logger.Errorf("unable to fetch NvmeOfStorage, err: %v", err)
			return reconcile.Result{}, err
		}

		// Retrieve the nvmeofstorage CR to update the CRUSH map
		for i := range r.nvmeOfStorage.Spec.Devices {
			device := &r.nvmeOfStorage.Spec.Devices[i]
			// Get OSD pods with label "app=rook-ceph-osd"
			var osdID, clusterName string
			opts := metav1.ListOptions{
				LabelSelector: "app=" + osd.AppName,
			}
			pods := r.getPods(context, request.Namespace, opts)
			// Find the osd id and cluster name for the fabric device listed in the nvmeofstorage CR
			for _, pod := range pods.Items {
				for _, envVar := range pod.Spec.Containers[0].Env {
					if envVar.Name == "ROOK_BLOCK_PATH" && envVar.Value == device.DeviceName {
						osdID = pod.Labels["ceph-osd-id"]
						clusterName = pod.Labels["app.kubernetes.io/part-of"]
						break
					}
				}
			}

			// Update CRUSH map for OSD relocation to fabric failure domain
			fabricHost := "fabric-host-" + r.nvmeOfStorage.Spec.Name
			clusterInfo := cephclient.AdminClusterInfo(context, request.Namespace, clusterName)
			cmd := []string{"osd", "crush", "move", "osd." + osdID, "host=" + fabricHost}
			exec := cephclient.NewCephCommand(r.context, clusterInfo, cmd)
			exec.JsonOutput = true
			buf, err := exec.Run()
			if err != nil {
				logger.Error(err, "Failed to move osd", "osdID", osdID, "srcHost", device.AttachedNode,
					"destHost", fabricHost, "result", string(buf))
				panic(err)
			}
			logger.Debugf("Successfully updated CRUSH Map. osdID: %s, srcHost: %s, destHost: %s",
				osdID, device.AttachedNode, fabricHost)

			// Update the AttachableHosts
			err = r.clustermanager.AddAttachbleHost(device.AttachedNode)
			if err != nil {
				panic(err)
			}
		}

		return reporting.ReportReconcileResult(logger, r.recorder, request, r.nvmeOfStorage, reconcile.Result{}, err)
		// TODO (cheolho.kang): Refactor this method to use a more reliable way of identifying the events,
		// such as checking labels or annotations, instead of string parsing.
	} else if strings.Contains(request.Name, "rook-ceph-osd") {
		// Get the attached hostname for the OSD
		opts := metav1.ListOptions{
			FieldSelector: "metadata.name=" + request.Name,
		}
		pods := r.getPods(context, request.Namespace, opts)
		attachedNode := pods.Items[0].Spec.NodeName

		// Get the next attachable hostname for the OSD
		nextHostName := r.clustermanager.GetNextAttachableHost(attachedNode)
		if nextHostName == "" {
			panic("no attachable hosts found")
		}
		logger.Debugf("Pod %q is going be transferred from %s to %s", request.Name, attachedNode, nextHostName)

		// TODO: Add create and run job for nvme-of device switch to the next host
		// Placeholder for the job creation and execution
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileNvmeOfStorage) getPods(context context.Context, namespace string, opts metav1.ListOptions) *corev1.PodList {
	pods, err := r.context.Clientset.CoreV1().Pods(namespace).List(context, opts)
	if err != nil || len(pods.Items) == 0 {
		panic(err)
	}
	return pods
}

func isOSDPod(labels map[string]string) bool {
	if labels["app"] == "rook-ceph-osd" && labels["ceph-osd-id"] != "" {
		return true
	}

	return false
}

func isPodDead(oldPod *corev1.Pod, newPod *corev1.Pod) bool {
	namespacedName := fmt.Sprintf("%s/%s", newPod.Namespace, newPod.Name)
	for _, cs := range newPod.Status.ContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
			logger.Infof("OSD Pod %q is in CrashLoopBackOff, oldPod.Status.Phase: %s", namespacedName, oldPod.Status.Phase)
			return true
		}
	}

	return false
}
