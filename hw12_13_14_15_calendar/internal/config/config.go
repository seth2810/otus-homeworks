package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// При желании конфигурацию можно вынести в internal/config.
// Организация конфига в main принуждает нас сужать API компонентов, использовать
// при их конструировании только необходимые параметры, а также уменьшает вероятность циклической зависимости.

type LoggerConf struct {
	Level, File string
}

type DatabaseConfig struct {
	Host, User, Password, DB string
	Port                     uint16
}

type RMQConfig struct {
	Host, User, Password string
	Port                 uint16
}

func ReadConfig(cfg interface{}, path string) error {
	v := viper.New()

	v.SetConfigFile(path)

	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("read error: %w", err)
	}

	if err := v.Unmarshal(cfg); err != nil {
		return fmt.Errorf("while unmarshal config: %w", err)
	}

	return nil
}
