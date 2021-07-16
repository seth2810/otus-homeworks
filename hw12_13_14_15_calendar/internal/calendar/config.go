package calendar

import (
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/config"
)

type ServerConf struct {
	HTTP HTTPConf
	GRPC GRPCConf
}

type HTTPConf struct {
	Host, Port string
}

type GRPCConf struct {
	Host, Port string
}

type StorageConfig struct {
	Type     string
	Database config.DatabaseConfig
}

type Config struct {
	Logger  config.LoggerConf
	Server  ServerConf
	Storage StorageConfig
}
