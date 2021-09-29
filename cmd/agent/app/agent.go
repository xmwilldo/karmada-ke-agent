package app

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	kubeclientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"

	"github.com/xmwilldo/karmada-ke-agent/cmd/agent/app/options"
	clusterv1alpha1 "github.com/xmwilldo/karmada-ke-agent/pkg/apis/cluster/v1alpha1"
	"github.com/xmwilldo/karmada-ke-agent/pkg/controllers/execution"
	"github.com/xmwilldo/karmada-ke-agent/pkg/controllers/mcs"
	"github.com/xmwilldo/karmada-ke-agent/pkg/controllers/status"
	"github.com/xmwilldo/karmada-ke-agent/pkg/karmadactl"
	"github.com/xmwilldo/karmada-ke-agent/pkg/util"
	"github.com/xmwilldo/karmada-ke-agent/pkg/util/gclient"
	"github.com/xmwilldo/karmada-ke-agent/pkg/util/helper"
	"github.com/xmwilldo/karmada-ke-agent/pkg/util/informermanager"
	"github.com/xmwilldo/karmada-ke-agent/pkg/util/names"
	"github.com/xmwilldo/karmada-ke-agent/pkg/util/objectwatcher"
)

// NewAgentCommand creates a *cobra.Command object with default parameters
func NewAgentCommand(ctx context.Context) *cobra.Command {
	opts := options.NewOptions()
	karmadaConfig := karmadactl.NewKarmadaConfig(clientcmd.NewDefaultPathOptions())

	cmd := &cobra.Command{
		Use:  "karmada-agent",
		Long: `The karmada agent runs the cluster registration agent`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := run(ctx, karmadaConfig, opts); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}
		},
	}

	opts.AddFlags(cmd.Flags())
	cmd.Flags().AddGoFlagSet(flag.CommandLine)
	return cmd
}

func run(ctx context.Context, karmadaConfig karmadactl.KarmadaConfig, opts *options.Options) error {
	klog.Infof("Agent version: %s", string("DebugStatusCollect:v2"))
	controlPlaneRestConfig, err := karmadaConfig.GetRestConfig(opts.KarmadaContext, opts.KarmadaKubeConfig)
	if err != nil {
		return fmt.Errorf("error building kubeconfig of karmada control plane: %s", err.Error())
	}
	controlPlaneRestConfig.QPS, controlPlaneRestConfig.Burst = opts.KubeAPIQPS, opts.KubeAPIBurst

	err = registerWithControlPlaneAPIServer(controlPlaneRestConfig, opts.Clusters)
	if err != nil {
		return fmt.Errorf("failed to register with karmada control plane: %s", err.Error())
	}

	//executionSpace, err := names.GenerateExecutionSpaceName(opts.ClusterName)
	//if err != nil {
	//	klog.Errorf("Failed to generate execution space name for member cluster %s, err is %v", opts.ClusterName, err)
	//	return err
	//}

	controllerManager, err := controllerruntime.NewManager(controlPlaneRestConfig, controllerruntime.Options{
		Scheme: gclient.NewSchema(),
		//Namespace:                  executionSpace,
		LeaderElection:             opts.LeaderElection.LeaderElect,
		LeaderElectionID:           fmt.Sprintf("karmada-agent-%s", "kubeedge-clusters"),
		LeaderElectionNamespace:    opts.LeaderElection.ResourceNamespace,
		LeaderElectionResourceLock: opts.LeaderElection.ResourceLock,
	})
	if err != nil {
		klog.Errorf("failed to build controller manager: %v", err)
		return err
	}

	setupControllers(controllerManager, opts, ctx.Done())

	// blocks until the context is done.
	if err := controllerManager.Start(ctx); err != nil {
		klog.Errorf("controller manager exits unexpectedly: %v", err)
		return err
	}

	return nil
}

func setupControllers(mgr controllerruntime.Manager, opts *options.Options, stopChan <-chan struct{}) {
	clusterStatusController := &status.ClusterStatusController{
		Client:                            mgr.GetClient(),
		KubeClient:                        kubeclientset.NewForConfigOrDie(mgr.GetConfig()),
		EventRecorder:                     mgr.GetEventRecorderFor(status.ControllerName),
		PredicateFunc:                     helper.NewClusterPredicateOnAgent(opts.Clusters),
		InformerManager:                   informermanager.GetInstance(),
		StopChan:                          stopChan,
		ClusterClientSetFunc:              util.NewClusterClientSetForAgent,
		ClusterDynamicClientSetFunc:       util.NewClusterDynamicClientSetForAgent,
		ClusterClientOption:               &util.ClientOption{QPS: opts.ClusterAPIQPS, Burst: opts.ClusterAPIBurst},
		ClusterStatusUpdateFrequency:      opts.ClusterStatusUpdateFrequency,
		ClusterLeaseDuration:              opts.ClusterLeaseDuration,
		ClusterLeaseRenewIntervalFraction: opts.ClusterLeaseRenewIntervalFraction,
	}
	if err := clusterStatusController.SetupWithManager(mgr); err != nil {
		klog.Fatalf("Failed to setup cluster status controller: %v", err)
	}

	caredNamespaces := make(sets.String, len(opts.Clusters))
	clusters := strings.Split(opts.Clusters, ",")
	for _, cluster := range clusters {
		executionSpace, err := names.GenerateExecutionSpaceName(cluster)
		if err != nil {
			klog.Errorf("failed to generate executionSpace for cluster: %s, skip watching this cluster", cluster)
			continue
		}
		caredNamespaces.Insert(executionSpace)
	}

	objectWatcher := objectwatcher.NewObjectWatcher(mgr.GetClient(), mgr.GetRESTMapper(), util.NewClusterDynamicClientSetForAgent)
	executionController := &execution.Controller{
		Client:               mgr.GetClient(),
		EventRecorder:        mgr.GetEventRecorderFor(execution.ControllerName),
		RESTMapper:           mgr.GetRESTMapper(),
		ObjectWatcher:        objectWatcher,
		PredicateFunc:        helper.NewExecutionPredicateOnGroupingAgent(caredNamespaces),
		ClusterClientSetFunc: util.NewClusterDynamicClientSetForAgent,
	}
	if err := executionController.SetupWithManager(mgr); err != nil {
		klog.Fatalf("Failed to setup execution controller: %v", err)
	}

	workStatusController := &status.WorkStatusController{
		Client:               mgr.GetClient(),
		EventRecorder:        mgr.GetEventRecorderFor(status.WorkStatusControllerName),
		RESTMapper:           mgr.GetRESTMapper(),
		InformerManager:      informermanager.GetInstance(),
		StopChan:             stopChan,
		WorkerNumber:         1,
		ObjectWatcher:        objectWatcher,
		PredicateFunc:        helper.NewExecutionPredicateOnGroupingAgent(caredNamespaces),
		ClusterClientSetFunc: util.NewClusterDynamicClientSetForAgent,
	}
	workStatusController.RunWorkQueue()
	if err := workStatusController.SetupWithManager(mgr); err != nil {
		klog.Fatalf("Failed to setup work status controller: %v", err)
	}

	serviceExportController := &mcs.ServiceExportController{
		Client:                      mgr.GetClient(),
		EventRecorder:               mgr.GetEventRecorderFor(mcs.ServiceExportControllerName),
		RESTMapper:                  mgr.GetRESTMapper(),
		InformerManager:             informermanager.GetInstance(),
		StopChan:                    stopChan,
		WorkerNumber:                1,
		PredicateFunc:               helper.NewPredicateForServiceExportControllerOnAgent(opts.Clusters),
		ClusterDynamicClientSetFunc: util.NewClusterDynamicClientSetForAgent,
	}
	serviceExportController.RunWorkQueue()
	if err := serviceExportController.SetupWithManager(mgr); err != nil {
		klog.Fatalf("Failed to setup ServiceExport controller: %v", err)
	}

	// Ensure the InformerManager stops when the stop channel closes
	go func() {
		<-stopChan
		informermanager.StopInstance()
	}()
}

func registerWithControlPlaneAPIServer(controlPlaneRestConfig *restclient.Config, Clusters string) error {
	client := gclient.NewForConfigOrDie(controlPlaneRestConfig)

	namespaceObj := &corev1.Namespace{}
	namespaceObj.Name = util.NamespaceClusterLease

	if err := util.CreateNamespaceIfNotExist(client, namespaceObj); err != nil {
		klog.Errorf("Failed to create namespace(%s) object, error: %v", namespaceObj.Name, err)
		return err
	}

	memberClusters := strings.Split(Clusters, ",")
	for _, memberClusterName := range memberClusters {
		clusterObj := &clusterv1alpha1.Cluster{}
		clusterObj.Name = memberClusterName
		clusterObj.Spec.SyncMode = clusterv1alpha1.Pull

		if err := util.CreateClusterIfNotExist(client, clusterObj); err != nil {
			klog.Errorf("Failed to create cluster(%s) object, error: %v", clusterObj.Name, err)
			return err
		}
	}

	return nil
}
