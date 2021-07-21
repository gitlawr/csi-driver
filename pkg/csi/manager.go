package csi

import (
	"github.com/rancher/wrangler/pkg/generated/controllers/core"
	"github.com/rancher/wrangler/pkg/kubeconfig"
	"k8s.io/client-go/rest"
	"kubevirt.io/client-go/kubecli"
)

type Manager struct {
	ids *IdentityServer
	ns  *NodeServer
	cs  *ControllerServer
}

func GetCSIManager() *Manager {
	return &Manager{}
}

func (m *Manager) Run(driverName, nodeID, endpoint, namespace, identityVersion, kubeConfig string) error {
	clientConfig := kubeconfig.GetNonInteractiveClientConfig(kubeConfig)
	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return err
	}
	coreClient, err := core.NewFactoryFromConfig(restConfig)
	if err != nil {
		return err
	}

	virtClient, err := kubecli.GetKubevirtClientFromRESTConfig(rest.CopyConfig(restConfig))
	if err != nil {
		return err
	}

	virtSubresourceClient, err := kubecli.GetKubevirtSubresourceClientFromFlags("", kubeConfig)
	if err != nil {
		return err
	}

	m.ids = NewIdentityServer(driverName, identityVersion)
	m.ns = NewNodeServer(coreClient.Core().V1(), virtClient, nodeID, namespace)
	m.cs = NewControllerServer(coreClient.Core().V1(), virtSubresourceClient, namespace)

	// Create GRPC servers
	s := NewNonBlockingGRPCServer()
	s.Start(endpoint, m.ids, m.cs, m.ns)
	s.Wait()

	return nil
}
