package communication

import (
	"context"
	"errors"
	commonCommunication "github.com/kulycloud/common/communication"
	"github.com/kulycloud/common/logging"
	protoCommon "github.com/kulycloud/protocol/common"
	protoServices "github.com/kulycloud/protocol/services"
	"github.com/kulycloud/service-manager-docker/config"
	"github.com/kulycloud/service-manager-docker/reconciling"
)

var ControlPlane *commonCommunication.ControlPlaneCommunicator

var _ protoServices.ServiceManagerServer = &ServiceManagerHandler{}

var ErrStorageNotReady = errors.New("storage is not ready")

var logger = logging.GetForComponent("handler")

type ReconcileFunc = func(context.Context, string) error

type ServiceManagerHandler struct {
	protoServices.UnimplementedServiceManagerServer
	reconciler ReconcileFunc
	listener *commonCommunication.Listener
}

func NewServiceManagerHandler(reconciler ReconcileFunc, listener *commonCommunication.Listener) *ServiceManagerHandler {
	return &ServiceManagerHandler{
		reconciler: reconciler,
		listener: listener,
	}
}

func (handler *ServiceManagerHandler) Register() {
	protoServices.RegisterServiceManagerServer(handler.listener.Server, handler)
}

func (handler *ServiceManagerHandler) Reconcile(ctx context.Context, request *protoServices.ReconcileRequest) (*protoCommon.Empty, error) {
	if !handler.listener.Storage.Ready() {
		return nil, ErrStorageNotReady
	}

	logger.Info("Starting reconcile!")
	return &protoCommon.Empty{}, handler.reconciler(ctx, request.Namespace)
}

func RegisterToControlPlane() {
	communicator := commonCommunication.RegisterToControlPlane("service-manager",
		config.GlobalConfig.Host, config.GlobalConfig.Port,
		config.GlobalConfig.ControlPlaneHost, config.GlobalConfig.ControlPlanePort)

	listener := commonCommunication.NewListener(logging.GetForComponent("listener"))

	//ctx := context.Background()
	r, err := reconciling.NewReconciler(listener.Storage)
	if err != nil {
		logger.Fatalw("could not connect to cluster: %w", err)
	}

	listener.NewStorageHandlers = append(listener.NewStorageHandlers, r.PropagateStorageToLoadBalancers)

	logger.Info("Starting listener")

	if err = listener.Setup(config.GlobalConfig.Port); err != nil {
		logger.Panicw("error initializing listener", "error", err)
	}

	handler := NewServiceManagerHandler(r.ReconcileDeployments, listener)
	handler.Register()

	//go r.WatchPods(context.Background())

	serveErr := listener.Serve()
	ControlPlane = <-communicator

	// listen on events
	err = ControlPlane.RegisterStorageChangedHandler(func(event *commonCommunication.StorageChanged) {
		r.PropagateStorageToLoadBalancers(context.Background(), event.Endpoints)
	})
	if err != nil {
		logger.Panicw("error registering for events", "error", err)
	}

	err = <-serveErr
	if err != nil {
		logger.Panicw("error serving listener", "error", err)
	}
}
