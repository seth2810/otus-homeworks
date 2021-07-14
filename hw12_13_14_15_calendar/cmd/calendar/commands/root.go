package commands

import (
	"context"
	"fmt"
	"net"
	"os/signal"
	"syscall"
	"time"

	"github.com/pressly/goose"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/app"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/config"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/logger"
	internalgrpc "github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/server/grpc"
	internalhttp "github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/server/http"
	memorystorage "github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/storage/memory"
	sqlstorage "github.com/seth2810/otus_homework/hw12_13_14_15_calendar/internal/storage/sql"
	"github.com/seth2810/otus_homework/hw12_13_14_15_calendar/migrations"
	"github.com/spf13/cobra"
)

var configFile string

var rootCmd = &cobra.Command{
	Use: "calendar",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.ReadConfig(configFile)
		if err != nil {
			return fmt.Errorf("failed to read config: %w", err)
		}

		return startApp(cmd.Context(), cfg)
	},
}

func init() {
	goose.AddNamedMigration("00001_create_events_table.go", migrations.Up0001, migrations.Down0001)

	rootCmd.Flags().StringVar(&configFile, "config", "/etc/calendar/config.yaml", "Path to configuration file")
	rootCmd.AddCommand(versionCmd)
}

func initStorage(ctx context.Context, cfg config.StorageConfig) (app.Storage, error) {
	switch cfg.Type {
	case "memory":
		return memorystorage.New(), nil
	case "sql":
		dsn := fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			cfg.Database.Host, cfg.Database.Port, cfg.Database.User, cfg.Database.Password, cfg.Database.DB,
		)

		db, err := goose.OpenDBWithDriver("postgres", dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to open DB: %w", err)
		}

		if err := goose.Up(db, "migrations"); err != nil {
			return nil, fmt.Errorf("failed to migrate: %w", err)
		}

		if storage, err := sqlstorage.New(dsn); err != nil {
			return nil, err
		} else if err := storage.Connect(ctx); err != nil {
			return nil, err
		} else {
			return storage, nil
		}
	default:
		return nil, fmt.Errorf("unrecognized type: %q", cfg.Type)
	}
}

func startApp(ctx context.Context, cfg *config.Config) error {
	log, err := logger.New(cfg.Logger.Level, cfg.Logger.File)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}

	storage, err := initStorage(ctx, cfg.Storage)
	if err != nil {
		return fmt.Errorf("failed to init storage: %w", err)
	}

	calendar := app.New(log, storage)

	grpcAddress := net.JoinHostPort(cfg.Server.GRPC.Host, cfg.Server.GRPC.Port)
	httpAddress := net.JoinHostPort(cfg.Server.HTTP.Host, cfg.Server.HTTP.Port)

	grpcServer := internalgrpc.NewServer(grpcAddress, log, calendar)
	httpServer := internalhttp.NewServer(httpAddress, grpcAddress, log)

	errCh := make(chan error, 2)

	go func() {
		<-ctx.Done()

		ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*3)

		defer cancelFn()

		log.Info("http is stopping...")

		if err := httpServer.Stop(ctx); err != nil {
			log.Error(fmt.Sprintln("failed to stop http server:", err))
		}

		log.Info("grpc is stopping...")

		if err := grpcServer.Stop(); err != nil {
			log.Error(fmt.Sprintln("failed to stop grpc server:", err))
		}

		errCh <- nil
	}()

	log.Info("calendar is running...")

	go func() {
		log.Info("grpc is running...")

		if err := grpcServer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("failed to run grpc server: %w", err)
		}
	}()

	go func() {
		log.Info("http is running...")

		if err := httpServer.Start(ctx); err != nil {
			errCh <- fmt.Errorf("failed to run http server: %w", err)
		}
	}()

	return <-errCh
}

func Execute() {
	ctx, cancelFn := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP,
	)

	defer cancelFn()

	cobra.CheckErr(rootCmd.ExecuteContext(ctx))
}
