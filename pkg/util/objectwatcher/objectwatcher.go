package objectwatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/xmwilldo/karmada-ke-agent/pkg/apis/cluster/v1alpha1"
	workv1alpha1 "github.com/xmwilldo/karmada-ke-agent/pkg/apis/work/v1alpha1"
	"github.com/xmwilldo/karmada-ke-agent/pkg/util"
	"github.com/xmwilldo/karmada-ke-agent/pkg/util/restmapper"
)

const (
	generationPrefix      = "gen:"
	resourceVersionPrefix = "rv:"
)

// ObjectWatcher manages operations for object dispatched to member clusters.
type ObjectWatcher interface {
	Create(cluster *v1alpha1.Cluster, desireObj *unstructured.Unstructured, workloadNeedsChange bool) error
	Update(cluster *v1alpha1.Cluster, desireObj, clusterObj *unstructured.Unstructured, workloadNeedsChange bool) error
	Delete(cluster *v1alpha1.Cluster, desireObj *unstructured.Unstructured, workloadNeedsChange bool) error
	NeedsUpdate(cluster *v1alpha1.Cluster, desiredObj, clusterObj *unstructured.Unstructured) (bool, error)
}

// ClientSetFunc is used to generate client set of member cluster
type ClientSetFunc func(c *v1alpha1.Cluster, client client.Client) (*util.DynamicClusterClient, error)

type objectWatcherImpl struct {
	Lock                 sync.RWMutex
	RESTMapper           meta.RESTMapper
	KubeClientSet        client.Client
	VersionRecord        map[string]map[string]string
	ClusterClientSetFunc ClientSetFunc
	createdNamespaces    sets.String
}

// NewObjectWatcher returns a instance of ObjectWatcher
func NewObjectWatcher(kubeClientSet client.Client, restMapper meta.RESTMapper, clusterClientSetFunc ClientSetFunc) ObjectWatcher {
	return &objectWatcherImpl{
		KubeClientSet:        kubeClientSet,
		VersionRecord:        make(map[string]map[string]string),
		RESTMapper:           restMapper,
		ClusterClientSetFunc: clusterClientSetFunc,
	}
}

func (o *objectWatcherImpl) Create(cluster *v1alpha1.Cluster, desireObj *unstructured.Unstructured, workloadNeedsChange bool) error {
	if workloadNeedsChange {
		if err := changeWorkload(cluster, desireObj); err != nil {
			return err
		}
	}

	dynamicClusterClient, err := o.ClusterClientSetFunc(cluster, o.KubeClientSet)
	if err != nil {
		klog.Errorf("Failed to build dynamic cluster client for cluster %s.", cluster.Name)
		return err
	}

	gvr, err := restmapper.GetGroupVersionResource(o.RESTMapper, desireObj.GroupVersionKind())
	if err != nil {
		klog.Errorf("Failed to create resource(%s/%s) as mapping GVK to GVR failed: %v", desireObj.GetNamespace(), desireObj.GetName(), err)
		return err
	}

	if err := o.createNamespaceIfNotExists(o.KubeClientSet, desireObj.GetNamespace()); err != nil {
		klog.Errorf("Failed to create obj for gvk: %s, namespaceName: %s/%s", desireObj.GetObjectKind().GroupVersionKind(), desireObj.GetNamespace(), desireObj.GetName())
		return err
	}
	// Karmada will adopt creating resource due to an existing resource in member cluster, because we don't want to force update or delete the resource created by users.
	// users should resolve the conflict in person.
	clusterObj, err := dynamicClusterClient.DynamicClientSet.Resource(gvr).Namespace(desireObj.GetNamespace()).Create(context.TODO(), desireObj, metav1.CreateOptions{})
	if err != nil {
		// The 'IsAlreadyExists' conflict may happen in following known scenarios:
		// - 1. In a reconcile process, the execution controller successfully applied resource to member cluster but failed to update the work conditions(Applied=True),
		//   when reconcile again, the controller will try to apply(by create) the resource again.
		// - 2. The resource already exist in the member cluster but it's not created by karmada.
		if apierrors.IsAlreadyExists(err) {
			existObj, err := dynamicClusterClient.DynamicClientSet.Resource(gvr).Namespace(desireObj.GetNamespace()).Get(context.TODO(), desireObj.GetName(), metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get exist resource(kind=%s, %s/%s) in cluster %v: %v", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), cluster.Name, err)
			}

			// Avoid updating resources that not managed by karmada.
			if util.GetLabelValue(desireObj.GetLabels(), workv1alpha1.WorkNameLabel) != util.GetLabelValue(existObj.GetLabels(), workv1alpha1.WorkNameLabel) {
				return fmt.Errorf("resource(kind=%s, %s/%s) already exist in cluster %v but not managed by karamda", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), cluster.Name)
			}

			return o.Update(cluster, desireObj, existObj, false)
		}
		klog.Errorf("Failed to create resource(kind=%s, %s/%s), err is %v ", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), err)
		return err
	}
	klog.Infof("Created resource(kind=%s, %s/%s) on cluster: %s", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), cluster.Name)

	// record version
	o.recordVersion(clusterObj, dynamicClusterClient.ClusterName)
	return nil
}

func (o *objectWatcherImpl) Update(cluster *v1alpha1.Cluster, desireObj, clusterObj *unstructured.Unstructured, workloadNeedsChange bool) error {
	if workloadNeedsChange {
		if err := changeWorkload(cluster, desireObj); err != nil {
			return err
		}
	}

	dynamicClusterClient, err := o.ClusterClientSetFunc(cluster, o.KubeClientSet)
	if err != nil {
		klog.Errorf("Failed to build dynamic cluster client for cluster %s.", cluster.Name)
		return err
	}

	gvr, err := restmapper.GetGroupVersionResource(o.RESTMapper, desireObj.GroupVersionKind())
	if err != nil {
		klog.Errorf("Failed to update resource(%s/%s) as mapping GVK to GVR failed: %v", desireObj.GetNamespace(), desireObj.GetName(), err)
		return err
	}

	err = RetainClusterFields(desireObj, clusterObj)
	if err != nil {
		klog.Errorf("Failed to retain fields for resource(kind=%s, %s/%s) : %v", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), err)
		return err
	}

	resource, err := dynamicClusterClient.DynamicClientSet.Resource(gvr).Namespace(desireObj.GetNamespace()).Update(context.TODO(), desireObj, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update resource(kind=%s, %s/%s), err is %v ", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), err)
		return err
	}

	klog.Infof("Updated resource(kind=%s, %s/%s) on cluster: %s", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), cluster.Name)

	// record version
	o.recordVersion(resource, cluster.Name)
	return nil
}

func (o *objectWatcherImpl) Delete(cluster *v1alpha1.Cluster, desireObj *unstructured.Unstructured, workloadNeedsChange bool) error {
	if workloadNeedsChange {
		if err := changeWorkload(cluster, desireObj); err != nil {
			return err
		}
	}
	dynamicClusterClient, err := o.ClusterClientSetFunc(cluster, o.KubeClientSet)
	if err != nil {
		klog.Errorf("Failed to build dynamic cluster client for cluster %s.", cluster.Name)
		return err
	}

	gvr, err := restmapper.GetGroupVersionResource(o.RESTMapper, desireObj.GroupVersionKind())
	if err != nil {
		klog.Errorf("Failed to delete resource(%s/%s) as mapping GVK to GVR failed: %v", desireObj.GetNamespace(), desireObj.GetName(), err)
		return err
	}

	err = dynamicClusterClient.DynamicClientSet.Resource(gvr).Namespace(desireObj.GetNamespace()).Delete(context.TODO(), desireObj.GetName(), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		err = nil
	}
	if err != nil {
		klog.Errorf("Failed to delete resource %v, err is %v ", desireObj.GetName(), err)
		return err
	}
	klog.Infof("Deleted resource(kind=%s, %s/%s) on cluster: %s", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), cluster.Name)

	objectKey := o.genObjectKey(desireObj)
	o.deleteVersionRecord(dynamicClusterClient.ClusterName, objectKey)

	return nil
}

func (o *objectWatcherImpl) genObjectKey(obj *unstructured.Unstructured) string {
	return obj.GroupVersionKind().String() + "/" + obj.GetNamespace() + "/" + obj.GetName()
}

// recordVersion will add or update resource version records
func (o *objectWatcherImpl) recordVersion(clusterObj *unstructured.Unstructured, clusterName string) {
	klog.Infof("[Debug]: recordVersion for clusterObj namespaceName: %s/%s", clusterObj.GetNamespace(), clusterObj.GetName())
	objVersion := objectVersion(clusterObj)
	objectKey := o.genObjectKey(clusterObj)
	if o.isClusterVersionRecordExist(clusterName) {
		o.updateVersionRecord(clusterName, objectKey, objVersion)
	} else {
		o.addVersionRecord(clusterName, objectKey, objVersion)
	}
}

// isClusterVersionRecordExist checks if the version record map of given member cluster exist
func (o *objectWatcherImpl) isClusterVersionRecordExist(clusterName string) bool {
	o.Lock.RLock()
	defer o.Lock.RUnlock()

	_, exist := o.VersionRecord[clusterName]

	return exist
}

// getVersionRecord will return the recorded version of given resource(if exist)
func (o *objectWatcherImpl) getVersionRecord(clusterName, resourceName string) (string, bool) {
	o.Lock.RLock()
	defer o.Lock.RUnlock()

	version, exist := o.VersionRecord[clusterName][resourceName]
	return version, exist
}

// addVersionRecord will add new version record of given resource
func (o *objectWatcherImpl) addVersionRecord(clusterName, resourceName, version string) {
	o.Lock.Lock()
	defer o.Lock.Unlock()
	o.VersionRecord[clusterName] = map[string]string{resourceName: version}
}

// updateVersionRecord will update the recorded version of given resource
func (o *objectWatcherImpl) updateVersionRecord(clusterName, resourceName, version string) {
	o.Lock.Lock()
	defer o.Lock.Unlock()
	o.VersionRecord[clusterName][resourceName] = version
}

// deleteVersionRecord will delete the recorded version of given resource
func (o *objectWatcherImpl) deleteVersionRecord(clusterName, resourceName string) {
	o.Lock.Lock()
	defer o.Lock.Unlock()
	delete(o.VersionRecord[clusterName], resourceName)
}

func (o *objectWatcherImpl) NeedsUpdate(cluster *v1alpha1.Cluster, desiredObj, clusterObj *unstructured.Unstructured) (bool, error) {
	// get resource version
	version, exist := o.getVersionRecord(cluster.Name, o.genObjectKey(clusterObj))
	if !exist {
		klog.Errorf("Failed to update resource %v/%v for the version record does not exist", desiredObj.GetNamespace(), desiredObj.GetName())
		return false, fmt.Errorf("failed to update resource %v/%v for the version record does not exist", desiredObj.GetNamespace(), desiredObj.GetName())
	}

	return objectNeedsUpdate(desiredObj, clusterObj, version), nil
}

func (o *objectWatcherImpl) createNamespaceIfNotExists(client client.Client, namespace string) error {
	if !o.createdNamespaces.Has(namespace) {
		namespaceObj := &corev1.Namespace{}
		namespaceObj.Name = namespace
		klog.Infof("Namespace %s does not exist, now create it.")
		if err := client.Create(context.TODO(), namespaceObj); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// This will happen when agent restarts losing createdNamespaces.
				return nil
			}
			return err
		}
		o.createdNamespaces.Insert(namespace)
	}
	// Namespace has already existed.
	return nil
}

/*
This code is lifted from the kubefed codebase. It's a list of functions to determines whether the provided cluster
object needs to be updated according to the desired object and the recorded version.
For reference: https://github.com/kubernetes-sigs/kubefed/blob/master/pkg/controller/util/propagatedversion.go#L30-L59
*/

// objectVersion retrieves the field type-prefixed value used for
// determining currency of the given cluster object.
func objectVersion(clusterObj *unstructured.Unstructured) string {
	generation := clusterObj.GetGeneration()
	if generation != 0 {
		return fmt.Sprintf("%s%d", generationPrefix, generation)
	}
	return fmt.Sprintf("%s%s", resourceVersionPrefix, clusterObj.GetResourceVersion())
}

// objectNeedsUpdate determines whether the 2 objects provided cluster
// object needs to be updated according to the desired object and the
// recorded version.
func objectNeedsUpdate(desiredObj, clusterObj *unstructured.Unstructured, recordedVersion string) bool {
	targetVersion := objectVersion(clusterObj)

	if recordedVersion != targetVersion {
		return true
	}

	// If versions match and the version is sourced from the
	// generation field, a further check of metadata equivalency is
	// required.
	return strings.HasPrefix(targetVersion, generationPrefix) && !objectMetaObjEquivalent(desiredObj, clusterObj)
}

// objectMetaObjEquivalent checks if cluster-independent, user provided data in two given ObjectMeta are equal. If in
// the future the ObjectMeta structure is expanded then any field that is not populated
// by the api server should be included here.
func objectMetaObjEquivalent(a, b metav1.Object) bool {
	if a.GetName() != b.GetName() {
		return false
	}
	if a.GetNamespace() != b.GetNamespace() {
		return false
	}
	aLabels := a.GetLabels()
	bLabels := b.GetLabels()
	if !reflect.DeepEqual(aLabels, bLabels) && (len(aLabels) != 0 || len(bLabels) != 0) {
		return false
	}
	return true
}

type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

// changeWorkload change the workload based on the workload kind
func changeWorkload(cluster *v1alpha1.Cluster, workload *unstructured.Unstructured) error {
	switch workload.GetKind() {
	case util.NamespaceKind:
		workload.SetName(fmt.Sprintf("%s-%s", workload.GetName(), cluster.Name))
	case util.DeploymentKind:
		overrides := []patchOperation{
			{
				Op:    "replace",
				Path:  "/metadata/namespace",
				Value: fmt.Sprintf("%s-%s", workload.GetNamespace(), cluster.Name),
			},
			{
				Op:    "add",
				Path:  "/spec/template/spec/nodeSelector",
				Value: cluster.Spec.NodeSelector,
			},
		}
		jsonPatchBytes, err := json.Marshal(overrides)
		if err != nil {
			return err
		}

		patch, err := jsonpatch.DecodePatch(jsonPatchBytes)
		if err != nil {
			return err
		}

		workloadJSONBytes, err := workload.MarshalJSON()
		if err != nil {
			return err
		}

		patchedObjectJSONBytes, err := patch.Apply(workloadJSONBytes)
		if err != nil {
			return err
		}

		if err := workload.UnmarshalJSON(patchedObjectJSONBytes); err != nil {
			return err
		}
	default:
		overrides := []patchOperation{
			{
				Op:    "replace",
				Path:  "/metadata/namespace",
				Value: fmt.Sprintf("%s-%s", workload.GetNamespace(), cluster.Name),
			},
		}
		jsonPatchBytes, err := json.Marshal(overrides)
		if err != nil {
			return err
		}

		patch, err := jsonpatch.DecodePatch(jsonPatchBytes)
		if err != nil {
			return err
		}

		workloadJSONBytes, err := workload.MarshalJSON()
		if err != nil {
			return err
		}

		patchedObjectJSONBytes, err := patch.Apply(workloadJSONBytes)
		if err != nil {
			return err
		}

		if err := workload.UnmarshalJSON(patchedObjectJSONBytes); err != nil {
			return err
		}
	}
	return nil
}
