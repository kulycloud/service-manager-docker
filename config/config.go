package config

import (
	commonConfig "github.com/kulycloud/common/config"
)

type Config struct {
	Host                    string `configName:"host"`
	Port                    uint32 `configName:"port"`
	ControlPlaneHost        string `configName:"controlPlaneHost"`
	ControlPlanePort        uint32 `configName:"controlPlanePort"`
	LoadBalancerImage       string `configName:"loadBalancerImage" defaultValue:"ghcr.io/kulycloud/load-balancer:latest-dev"`
	LoadBalancerControlPort uint32 `configName:"loadBalancerControlPort" defaultValue:"12270"`
	HTTPPort                uint32 `configName:"httpPort" defaultValue:"30000"`
	LocalHostFromDocker     string `configName:"localHostFromDocker" defaultValue:"127.0.0.1"`
}

var GlobalConfig = &Config{}

func ParseConfig() error {
	parser := commonConfig.NewParser()
	parser.AddProvider(commonConfig.NewCliParamProvider())
	parser.AddProvider(commonConfig.NewEnvironmentVariableProvider())

	return parser.Populate(GlobalConfig)
}
