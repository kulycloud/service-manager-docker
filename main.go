package main

import (
	"context"
	commonCommunication "github.com/kulycloud/common/communication"
	"github.com/kulycloud/common/logging"
	"github.com/kulycloud/service-manager-docker/communication"
	"github.com/kulycloud/service-manager-docker/config"
	"github.com/kulycloud/service-manager-docker/reconciling"
)

var logger = logging.GetForComponent("init")

func main() {
	defer logging.Sync()

	err := config.ParseConfig()
	if err != nil {
		logger.Fatalw("Error parsing config", "error", err)
	}
	logger.Infow("Finished parsing config")
	RegisterToControlPlane()
}

func RegisterToControlPlane() {
	communicator := commonCommunication.RegisterToControlPlane("service-manager",
		config.GlobalConfig.Host, config.GlobalConfig.Port,
		config.GlobalConfig.ControlPlaneHost, config.GlobalConfig.ControlPlanePort, true)

	listener := commonCommunication.NewListener(logging.GetForComponent("listener"))

	logger.Info("Starting listener")

	if err := listener.Setup(config.GlobalConfig.Port); err != nil {
		logger.Panicw("error initializing listener", "error", err)
	}

	handler := communication.NewServiceManagerHandler(listener)
	handler.Register()

	serveErr := listener.Serve()
	communication.ControlPlane = <-communicator

	r, err := reconciling.NewReconciler(communication.ControlPlane.Storage)
	if err != nil {
		logger.Fatalw("could not connect to cluster: %w", err)
	}

	if err != nil {
		logger.Fatalw("could not setup cluster: %w", err)
	}

	handler.Reconciler = r.ReconcileDeployments
	// listen on events
	err = communication.ControlPlane.RegisterStorageChangedHandler(func(event *commonCommunication.StorageChanged) {
		r.PropagateStorageToLoadBalancers(context.Background(), event.Endpoints)
	})
	if err != nil {
		logger.Panicw("error registering for events", "error", err)
	}

	//go r.WatchPods(context.Background())

	err = <-serveErr
	if err != nil {
		logger.Panicw("error serving listener", "error", err)
	}
}
