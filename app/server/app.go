package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-playground/validator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelseyhightower/envconfig"
	"gitlab.com/navyx/nexus/nexus-store/pkg/grpc"
	nexus "gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
	"gitlab.com/navyx/nexus/nexus-store/pkg/storage"
	grpc_grpc "google.golang.org/grpc"
)

const appName string = "nexus-store"
const bootstrapApiToken string = "bootstrap-token"

type LoggingQueryTracer struct {
	logger *slog.Logger
}

func (t *LoggingQueryTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	traceId := ctx.Value("TRACEID")
	if strings.Contains(data.SQL, "api_tokens") {
		t.logger.Debug("Query started", "sql", data.SQL, "TraceId", traceId)
	} else {
		t.logger.Debug("Query started", "sql", data.SQL, "args", data.Args, "TraceId", traceId)
	}
	return ctx
}

func (t *LoggingQueryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	traceId := ctx.Value("TRACEID")
	t.logger.Debug("Query ended", "CommandTag", data.CommandTag, "duration", data.CommandTag, "TraceId", traceId)
}

type App struct{}

func (a *App) Run() {
	ctx := context.Background()

	config := a.loadConfig()

	// Connect to the database and create a new accessor
	dbConfig, err := pgxpool.ParseConfig(config.DatabaseUrl)
	if err != nil {
		slog.Error("Failed to parse connection string", "err", err)
		os.Exit(1)
	}

	dbConfig.ConnConfig.Tracer = &LoggingQueryTracer{slog.Default()}
	pool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		slog.Error("Failed to connect to database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	slog.Info("Connected to database", "database", config.DatabaseUrl)

	nexusStoreGrpcListen, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.Port))
	if err != nil {
		slog.Error("Failed to listen to nexus store", "err", err)
		os.Exit(1)
	}
	defer nexusStoreGrpcListen.Close()

	var s3Storage *storage.S3Storage
	s3Storage, err = storage.NewS3Storage(ctx, config.FileS3Bucket)
	if err != nil {
		slog.Error("Fail to create S3Storage", "err", err)
		os.Exit(1)
	}

	// r := retry.New(func(error) bool { return true }, 10, 500, 3000)

	// err = r.Do(
	// 	func() error {
	// 		err = s3Storage.SetCORS(ctx, []string{"*"}, []string{"GET", "HEAD"}, 3000, []string{"Authorization"})
	// 		if err != nil {
	// 			return fmt.Errorf("fail to call S3Storage.SetCORS. %w", err)
	// 		}
	// 		// err = s3Storage.EnableExpiration(1) // Uncomment this line to enable auto_expire
	// 		err = s3Storage.DeleteBucketLifecycle(ctx) // Remove all lifecycle rules to disable auto_expire
	// 		if err != nil {
	// 			return fmt.Errorf("fail to call S3Storage.EnableExpiration. %w", err)
	// 		}
	// 		return nil
	// 	},
	// )
	// if err != nil {
	// 	slog.Error(err.Error())
	// 	os.Exit(1)
	// }

	nexusStoreGrpcServer := grpc_grpc.NewServer(
		grpc_grpc.MaxRecvMsgSize(1024 * 1024 * 10 * 11 / 10),
	)
	nexusStoreGrpc := grpc.NewNexusStoreServer(
		grpc.WithDataSource(pool),
		grpc.WithStorage(s3Storage),
	)
	nexus.RegisterNexusStoreServer(nexusStoreGrpcServer, nexusStoreGrpc)

	go func() {
		slog.Info("Nexus store server listening on port", "port", config.Port)
		err := nexusStoreGrpcServer.Serve(nexusStoreGrpcListen)
		if err != nil {
			slog.Error("nexusStoreGrpcServer.Serve() returns errors", "err", err)
			os.Exit(1)
		}
	}()

	a._WaitForInteruption()

	slog.Info("Shutting down nexus store server......")
	nexusStoreGrpcServer.GracefulStop()
}

func (a *App) _WaitForInteruption() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}

func (a *App) loadConfig() Config {
	// Load environment variables into the struct
	var config Config
	if err := envconfig.Process("", &config); err != nil {
		slog.Error("Failed to process environment variables.", "err", err)
		os.Exit(1)
	}

	// Validate the struct
	validate := validator.New()
	if err := validate.Struct(config); err != nil {
		slog.Error("Validation failed", "err", err)
		os.Exit(1)
	}

	// construct database url from the environment variables
	if config.DatabaseUrl == "" {
		config.DatabaseUrl = fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s",
			config.DatabaseUser,
			config.DatabasePassword,
			config.DatabaseHost,
			config.DatabasePort,
			config.DatabaseName)
	}

	return config
}
