package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/KonstantinPavlov/verification/internal/core"
	"github.com/KonstantinPavlov/verification/internal/http_server"
	"github.com/KonstantinPavlov/verification/internal/logger"
	"github.com/labstack/echo/v4"
)

type AppConfig struct {
	Server struct {
		Port int `yaml:"port"`
	} `yaml:"server"`
}

type AppResources struct {
	Logger     *logger.Logger
	AppConfig  *AppConfig
	HttpServer http_server.HttpServer
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	container := core.NewContainer[AppResources](ctx)
	if err := container.Start(
		setUpLogger,
		setUpAppConfig,
		setUpHttpServer,
	); err != nil {
		if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "interrupt") {
			if container.GetResources().Logger != nil {
				container.GetResources().Logger.Info("App gracefully stopped")
			} else {
				log.Println("App gracefully stopped")
			}
			return
		}
		log.Fatalf("Critical failure starting app: %v", err)
	}
}

func setUpLogger(c core.Container[AppResources]) core.StopFn {
	c.GetResources().Logger = logger.New(
		logger.NewDefaultBuilder().LevelStr("DEBUG").Build(os.Stdout),
	)
	c.GetResources().Logger.Info("Logger has been initialized!")
	return func() {}
}

func setUpAppConfig(c core.Container[AppResources]) core.StopFn {
	var config = AppConfig{}
	config.Server.Port = 8080
	c.GetResources().AppConfig = &config
	//TODO read from yaml other config!
	return func() {}
}

func setUpHttpServer(c core.Container[AppResources]) core.StopFn {
	var AppRoutes http_server.Router = func(e *echo.Echo) {
		e.GET("/", func(e echo.Context) error {
			c.GetResources().Logger.Info("Hello from cutom route!")
			return e.String(http.StatusOK, "Hello from custom route")
		})
	}

	serverOpts := http_server.DefaultHttpServerOptions()
	serverOpts.ConnectedRoutes = AppRoutes
	serverOpts.Address = fmt.Sprintf("0.0.0.0:%d", c.GetResources().AppConfig.Server.Port)
	c.GetResources().HttpServer = http_server.NewHttpServer(serverOpts)

	go func() {
		if err := c.GetResources().HttpServer.Start(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				c.GetResources().Logger.Error("Start of http server failed!", "err", err)
				c.Stop(err)
			}
		}
	}()
	return func() {
		c.GetResources().HttpServer.Stop()
	}
}
