package reconciling

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/go-connections/nat"
	commonCommunication "github.com/kulycloud/common/communication"
	"github.com/kulycloud/common/logging"
	protoCommon "github.com/kulycloud/protocol/common"
	protoStorage "github.com/kulycloud/protocol/storage"
	"github.com/kulycloud/service-manager-docker/config"
	"github.com/kulycloud/service-manager-k8s/communication"
	"github.com/moby/moby/client"
	"net"
	"strconv"
	"time"
)

const (
	labelPrefix      = "platform.kuly.cloud/"
	namespaceLabel   = labelPrefix + "namespace"
	typeLabel        = labelPrefix + "type"
	typeLabelService = "service"
	typeLabelLB      = "loadbalancer"
	nameLabel        = labelPrefix + "name"
	rpcPortLabel     = labelPrefix + "rpcPort"
	httpPortLabel    = labelPrefix + "httpPort"
)

const originOutsideDocker = "127.0.0.1"

var logger = logging.GetForComponent("reconciler")

type Reconciler struct {
	storage *commonCommunication.StorageCommunicator
	client *client.Client
	nextPort int
}

func NewReconciler(storage *commonCommunication.StorageCommunicator) (*Reconciler, error) {

	cli, err := client.NewEnvClient()

	if err != nil {
		return nil, err
	}

	return &Reconciler{
		storage: storage,
		client: cli,
		nextPort: 40000,
	}, nil
}

type serviceContainerList struct {
	lbs 	 []types.Container
	services []types.Container
}

func (r *Reconciler) ReconcileDeployments(ctx context.Context, namespace string) error {
	filterArgs := filters.NewArgs()
	filterArgs.Add("label", fmt.Sprintf("%s=%s", namespaceLabel, namespace))
	containers, err := r.client.ContainerList(ctx, types.ContainerListOptions{
		Filters: filterArgs,
	})

	if err != nil {
		return err
	}

	logger.Infow("found containers", "containers", containers)

	serviceContainers := make(map[string]*serviceContainerList)

	for _, cont := range containers {
		serviceName := cont.Labels[nameLabel]
		cList, ok := serviceContainers[serviceName]
		if !ok {
			cList = &serviceContainerList{
				lbs:      make([]types.Container, 0),
				services: make([]types.Container, 0),
			}
			serviceContainers[serviceName] = cList
		}

		if cont.Labels[typeLabel] == typeLabelLB {
			cList.lbs = append(cList.lbs, cont)
		} else if cont.Labels[typeLabel] == typeLabelService {
			cList.services = append(cList.services, cont)
		}

	}


	services, err := r.storage.GetServicesInNamespace(ctx, namespace)

	for _, serviceName := range services {
		cList, ok := serviceContainers[serviceName]
		if !ok {
			cList = &serviceContainerList{
				lbs:      make([]types.Container, 0),
				services: make([]types.Container, 0),
			}
		}
		err := r.processService(ctx, cList, namespace, serviceName)
		if err != nil {
			logger.Warnw("error reconciling service", "error", err, "namespace", namespace, "service", serviceName)
		}
		delete(serviceContainers, serviceName)
	}

	for serviceName, cList := range serviceContainers {
		err := r.storage.SetServiceLBEndpoints(ctx, namespace, serviceName, []*protoCommon.Endpoint{})
		if err != nil {
			return err
		}
		err = r.StopContainers(ctx, cList.lbs)
		if err != nil {
			return err
		}
		err = r.StopContainers(ctx, cList.services)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Reconciler) processService(ctx context.Context, cList *serviceContainerList, namespace string, serviceName string) error {
	service, err := r.storage.GetService(ctx, namespace, serviceName)

	if err != nil {
		return err
	}

	lbEndpoints := make([]*protoCommon.Endpoint, 0)
	lbHttpEndpoints := make([]*protoCommon.Endpoint, 0)
	svcEndpoints := make([]*protoCommon.Endpoint, 0)

	// ensure load balancers
	if len(cList.lbs) < 2 {
		for _, c := range cList.lbs {
			lbEndpoints = append(lbEndpoints, getHttpEndpoint(&c, originOutsideDocker))
			lbHttpEndpoints = append(lbHttpEndpoints, getHttpEndpoint(&c, config.GlobalConfig.LocalHostFromDocker))
		}
		i := len(cList.lbs)
		for ;i < 2; i++ {
			rpcEndpoint, httpEndpoint, err := r.StartLoadBalancer(ctx, namespace, serviceName)
			if err != nil {
				return err
			}
			lbEndpoints = append(lbEndpoints, rpcEndpoint)
			lbHttpEndpoints = append(lbHttpEndpoints, httpEndpoint)
		}
	} else {
		for i := 0 ;i < 2; i++ {
			lbEndpoints = append(lbEndpoints, getRpcEndpoint(&cList.lbs[i], originOutsideDocker))
			lbHttpEndpoints = append(lbEndpoints, getHttpEndpoint(&cList.lbs[i], config.GlobalConfig.LocalHostFromDocker))
		}
		err := r.StopContainers(ctx, cList.lbs[2:])
		if err != nil {
			return err
		}
	}

	// ensure service containers
	n := int(service.Replicas)
	if len(cList.services) < n {
		for _, c := range cList.services {
			svcEndpoints = append(svcEndpoints, getHttpEndpoint(&c, config.GlobalConfig.LocalHostFromDocker))
		}
		i := len(cList.services)
		for ; i < n; i++ {
			endpoint, err := r.StartServiceContainer(ctx, namespace, serviceName, service)
			if err != nil {
				return err
			}
			svcEndpoints = append(svcEndpoints, endpoint)
		}
		// start until we have n
	} else {
		for i := 0 ;i < n; i++ {
			svcEndpoints = append(svcEndpoints, getHttpEndpoint(&cList.services[i], config.GlobalConfig.LocalHostFromDocker))
		}
		err := r.StopContainers(ctx, cList.services[n:])
		if err != nil {
			return err
		}
	}

	err = r.storage.SetServiceLBEndpoints(ctx, namespace, serviceName, lbHttpEndpoints)
	if err != nil {
		return err
	}

	comm, err := communication.NewMultiLoadBalancerCommunicator(lbEndpoints)
	if err != nil {
		return err
	}

	return comm.Update(ctx, svcEndpoints, convertEndpointToInsideDocker(r.storage.Endpoints))
}

func convertEndpointToInsideDocker(endpoints []*protoCommon.Endpoint) []*protoCommon.Endpoint {
	ep := make([]*protoCommon.Endpoint, 0)
	for _, endpoint := range endpoints {
		ep = append(ep, &protoCommon.Endpoint{
			Host: config.GlobalConfig.LocalHostFromDocker,
			Port: endpoint.Port,
		})
	}

	return ep
}

func (r *Reconciler) StopContainers(ctx context.Context, containers []types.Container) error {
	for _, cont := range containers {
		err := r.client.ContainerStop(ctx, cont.ID, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func getHttpEndpoint(cont *types.Container, origin string) *protoCommon.Endpoint {
	port, _ := strconv.Atoi(cont.Labels[httpPortLabel])
	return &protoCommon.Endpoint{Host: origin, Port: uint32(port)}
}

func getRpcEndpoint(cont *types.Container, origin string) *protoCommon.Endpoint {
	port, _ := strconv.Atoi(cont.Labels[rpcPortLabel])
	return &protoCommon.Endpoint{Host: origin, Port: uint32(port)}
}

func (r *Reconciler) StartServiceContainer(ctx context.Context, namespace string, serviceName string, service *protoStorage.Service) (*protoCommon.Endpoint, error) {
	env := make([]string, 0)
	for key, val := range service.Environment {
		env = append(env, fmt.Sprintf("%s=%s", key, val))
	}

	if len(service.Arguments) > 0 {
		logger.Warnw("Arguments not supported in service-manager-docker", "namespace", namespace, "service", serviceName)
	}

	port, err := r.getNextPort()
	if err != nil {
		return nil, err
	}

	httpPort, err := nat.NewPort("tcp", fmt.Sprintf("%v",config.GlobalConfig.HTTPPort))
	if err != nil {
		return nil, err
	}

	portMapping := nat.PortBinding{HostIP: "127.0.0.1", HostPort: fmt.Sprintf("%v", port)}


	resp, err := r.client.ContainerCreate(ctx, &container.Config{
		Labels: map[string]string{
			namespaceLabel: namespace,
			nameLabel:      serviceName,
			typeLabel:      typeLabelService,
			httpPortLabel:  fmt.Sprintf("%v", port),
		},
		Image: service.Image,
		Env: env,
		ExposedPorts: map[nat.Port]struct{}{
			httpPort: {},
		},
	}, &container.HostConfig{
		PortBindings: map[nat.Port][]nat.PortBinding{
			httpPort: { portMapping },
		},
	}, nil, "")

	if err != nil {
		return nil, err
	}

	if err := r.client.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		return nil, err
	}

	return &protoCommon.Endpoint{Host: config.GlobalConfig.LocalHostFromDocker, Port: uint32(port)}, nil
}

func (r *Reconciler) StartLoadBalancer(ctx context.Context, namespace string, serviceName string) (*protoCommon.Endpoint, *protoCommon.Endpoint, error) {
	httpHostPort, err := r.getNextPort()
	if err != nil {
		return nil, nil, err
	}

	httpPort, err := nat.NewPort("tcp", fmt.Sprintf("%v",config.GlobalConfig.HTTPPort))
	if err != nil {
		return nil, nil, err
	}

	httpPortMapping := nat.PortBinding{HostIP: "127.0.0.1", HostPort: fmt.Sprintf("%v", httpHostPort)}

	rpcHostPort, err := r.getNextPort()
	if err != nil {
		return nil, nil, err
	}

	rpcPort, err := nat.NewPort("tcp", fmt.Sprintf("%v",config.GlobalConfig.LoadBalancerControlPort))
	if err != nil {
		return nil, nil, err
	}

	rpcPortMapping := nat.PortBinding{HostIP: "127.0.0.1", HostPort: fmt.Sprintf("%v", rpcHostPort)}

	resp, err := r.client.ContainerCreate(ctx, &container.Config{
		Labels: map[string]string{
			namespaceLabel: namespace,
			nameLabel:      serviceName,
			typeLabel:      typeLabelLB,
			httpPortLabel:  fmt.Sprintf("%v", httpHostPort),
			rpcPortLabel:   fmt.Sprintf("%v", rpcHostPort),
		},
		ExposedPorts: map[nat.Port]struct{}{
			httpPort: {},
			rpcPort: {},
		},
		Env: []string{
			fmt.Sprintf("PORT=%v", config.GlobalConfig.LoadBalancerControlPort),
		},
		Image: config.GlobalConfig.LoadBalancerImage,
	}, &container.HostConfig{
		PortBindings: map[nat.Port][]nat.PortBinding{
			httpPort: {httpPortMapping},
			rpcPort: {rpcPortMapping},
		},
	}, nil, "")

	if err != nil {
		return nil, nil, err
	}

	if err := r.client.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		return nil, nil, err
	}

	return &protoCommon.Endpoint{Host: originOutsideDocker, Port: uint32(rpcHostPort)}, &protoCommon.Endpoint{Host: config.GlobalConfig.LocalHostFromDocker, Port: uint32(httpHostPort)}, nil
}

func (r *Reconciler) getNextPort() (int, error) {
	logger.Info("Searching usable port")
	for {
		port := r.nextPort
		r.nextPort++

		portStr := fmt.Sprintf("%v", port)

		timeout := time.Second
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", portStr), timeout)
		if err != nil {
			logger.Infow("Found usable port", "port", port)
			return port, nil
		}

		if conn != nil {
			_ = conn.Close()
			logger.Infow("Port in use", "port", port)
			continue
		}
		logger.Info("Found usable port", "port", port)
		return port, nil

	}
}

func (r *Reconciler) PropagateStorageToLoadBalancers(ctx context.Context, endpoints []*protoCommon.Endpoint) {

}
