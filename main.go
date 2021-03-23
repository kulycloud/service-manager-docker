package main

import (
	commonCommunication "github.com/kulycloud/common/communication"
	"github.com/kulycloud/common/logging"
	"github.com/kulycloud/service-manager-docker/communication"
	"github.com/kulycloud/service-manager-docker/config"
	"github.com/kulycloud/service-manager-docker/reconciling"
	k8sReconciling "github.com/kulycloud/service-manager-k8s/reconciling"
)

var logger = logging.GetForComponent("init")

func main() {
	defer logging.Sync()

	err := config.ParseConfig()
	if err != nil {
		logger.Fatalw("Error parsing config", "error", err)
	}
	logger.Infow("Finished parsing config")
	handlerErrStream := RegisterToControlPlane()
	scheduler := CreateSchedulerWithReconciler()
	schedulerErrStream := scheduler.Start()

	select {
	case err = <-handlerErrStream:
		logger.Panicw("error serving listener", "error", err)
	case err = <-schedulerErrStream:
		logger.Panicw("error in scheduler", "error", err)
	}

	// die on error
}

func RegisterToControlPlane() <-chan error {
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

	return serveErr
}

func CreateSchedulerWithReconciler() *k8sReconciling.ReconcileScheduler {
	reconciler, err := reconciling.NewDockerReconciler(communication.ControlPlane.Storage)
	if err != nil {
		logger.Fatalw("could not create reconciler", "error", err)
	}

	scheduler, err := k8sReconciling.NewReconcilerScheduler(communication.ControlPlane.Storage, reconciler)
	if err != nil {
		logger.Fatalw("could not connect to cluster", "error", err)
	}

	err = scheduler.RegisterEventHandlers(communication.ControlPlane)
	if err != nil {
		logger.Panicw("error registering for storage events", "error", err)
	}

	return scheduler
}

