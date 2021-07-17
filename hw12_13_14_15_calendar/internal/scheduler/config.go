package scheduler

import (
	"time"

	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/config"
)

type Config struct {
	Database config.DatabaseConfig
	Logger   config.LoggerConf
	RMQ      config.RMQConfig
	Interval time.Duration
}
