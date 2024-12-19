package main

import (
	"log/slog"
	"net/http"
	"os"

	"ex.com/internal/config"
	getall "ex.com/internal/http-server/handlers/url/get_all"
	"ex.com/internal/http-server/handlers/url/redirect"
	"ex.com/internal/http-server/handlers/url/save"
	"ex.com/internal/lib/loggeer/handlers/slogpretty"
	"ex.com/internal/lib/loggeer/sl"
	"ex.com/internal/storage/sqlite"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

func main() {
	cfg := config.MustLoad()
	logger := setupLogger(cfg.Env)
	logger.Debug("Logger messages are enabled")
	logger.Info("Server is starting", slog.String("env", cfg.Env), slog.String("address", cfg.Address))

	storage, err := sqlite.New(cfg.StoragePath)
	if err != nil {
		logger.Error("failed to init storage", sl.Err(err))
		os.Exit(1)
	}
	_ = storage

	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(middleware.Logger)
	router.Use(middleware.Recoverer)
	router.Use(middleware.URLFormat)

	router.Post("/url/save", save.New(logger, storage))
	router.Get("/url/getAll", getall.New(logger, storage))
	router.Get("/{alias}", redirect.New(logger, storage))
	server := &http.Server{
		Addr:         cfg.Address,
		Handler:      router,
		ReadTimeout:  cfg.HTTPServer.Timeout,
		WriteTimeout: cfg.HTTPServer.Timeout,
		IdleTimeout:  cfg.HTTPServer.IdleTimeout,
	}

	if err := server.ListenAndServe(); err != nil {
		logger.Error("failed to init server", sl.Err(err))
		os.Exit(1)
	}

	logger.Info("Server stoped")

}

func setupLogger(env string) *slog.Logger {
	var log *slog.Logger

	switch env {
	case envLocal:
		log = setupPrettyLog()
	case envDev:
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	case envProd:
		log = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	return log

}

func setupPrettyLog() *slog.Logger {
	opts := slogpretty.PrettyHandlerOptions{
		SlogOpts: &slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	}

	handler := opts.NewPrettyHandler(os.Stdout)

	return slog.New(handler)
}
