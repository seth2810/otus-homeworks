package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// При желании конфигурацию можно вынести в internal/config.
// Организация конфига в main принуждает нас сужать API компонентов, использовать
// при их конструировании только необходимые параметры, а также уменьшает вероятность циклической зависимости.
type Config struct {
	Logger  LoggerConf
	Server  ServerConf
	Storage StorageConfig
}

type LoggerConf struct {
	Level, File string
}

type StorageConfig struct {
	Type     string
	Database DatabaseConfig
}

type DatabaseConfig struct {
	Host, User, Password, DB string
	Port                     uint16
}

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

func ReadConfig(path string) (*Config, error) {
	cfg := &Config{}

	v := viper.New()

	v.SetConfigFile(path)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read error: %w", err)
	}

	if err := v.Unmarshal(cfg); err != nil {
		return cfg, fmt.Errorf("while unmarshal config: %w", err)
	}

	return cfg, nil
}
