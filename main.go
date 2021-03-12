package main

import (
	"github.com/kulycloud/common/logging"
	"github.com/kulycloud/service-manager-docker/communication"
	"github.com/kulycloud/service-manager-docker/config"
)

var logger = logging.GetForComponent("init")

func main() {
	defer logging.Sync()

	err := config.ParseConfig()
	if err != nil {
		logger.Fatalw("Error parsing config", "error", err)
	}
	logger.Infow("Finished parsing config")

	communication.RegisterToControlPlane()
}
