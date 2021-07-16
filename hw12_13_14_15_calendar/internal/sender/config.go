package sender

import "github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/config"

type Config struct {
	Logger   config.LoggerConf
	RMQ      config.RMQConfig
	Queue    string
	Database config.DatabaseConfig
}
