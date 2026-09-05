package http_server

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/KonstantinPavlov/verification/internal/logger"
	echoprometheus "github.com/labstack/echo-prometheus"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	healthCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "health_check_counter_total",
			Help: "Counter which indicates count of helath invocations",
		},
	)
)

const NAME = "name"
const HOST = "host"

type Router func(e *echo.Echo)

type HttpServer interface {
	Start(ctx context.Context) (err error)
	Stop()
}

type HttpServerOptions struct {
	Name            string
	Address         string
	ConnectedRoutes Router
	Log             *logger.Logger
	HealthURI       string
	MetricURI       string
}

func DefaultHttpServerOptions() (opts *HttpServerOptions) {
	return &HttpServerOptions{
		Name:    "http_server",
		Address: "0.0.0.0:8080",
		ConnectedRoutes: func(e *echo.Echo) {
			//no default routes!
		},
		Log:       logger.New(),
		HealthURI: "/health",
		MetricURI: "/metrics",
	}
}

type server struct {
	opt  *HttpServerOptions
	echo *echo.Echo
}

func (s *server) Start(ctx context.Context) (err error) {
	s.opt.Log.Info("Start http server", NAME, s.opt.Name, HOST, s.opt.Address)
	print("\x1b[36m" +
		"   ____    __\n" +
		"  / __/___/ /  ___\n" +
		" / _// __/ _ \\/ _ \\\n" +
		"/___/\\__/_//_/\\___/ \n" +
		"High performance, minimalist Go web framework\n" +
		"https://labstack.com\n\n\x1b[0m")
	sc := echo.StartConfig{
		Address:         s.opt.Address,
		GracefulTimeout: 10 * time.Second,
		HideBanner:      false,
		HidePort:        false,
	}
	return sc.Start(ctx, s.echo)
}

func (s server) Stop() {
	s.opt.Log.Info("Stop http server", NAME, s.opt.Name, HOST, s.opt.Address)
}

func NewHttpServer(opts *HttpServerOptions) (s HttpServer) {
	srv := &server{echo: echo.New(), opt: opts}
	srv.echo.Logger = opts.Log.Logger
	srv.opt.ConnectedRoutes(srv.echo)

	// logging middleware
	srv.echo.Use(
		middleware.RequestLoggerWithConfig(
			middleware.RequestLoggerConfig{
				LogStatus: true,
				LogURI:    true,
				//LogError:      true,
				HandleError:   true,
				LogLatency:    true,
				LogValuesFunc: logValuesFunc(opts),
			},
		),
	)
	// Prometehus metrics middleware
	srv.echo.Use(
		echoprometheus.NewMiddlewareWithConfig(
			echoprometheus.MiddlewareConfig{
				Subsystem:  opts.Name,
				Registerer: prometheus.DefaultRegisterer,
			},
		),
	)
	// MetricURI handler
	srv.echo.GET(
		srv.opt.MetricURI,
		echoprometheus.NewHandlerWithConfig(
			echoprometheus.HandlerConfig{Gatherer: prometheus.DefaultGatherer},
		),
	)
	// HealthURI handler
	srv.echo.GET(
		srv.opt.HealthURI,
		health,
	)
	return srv
}

func health(c *echo.Context) error {
	healthCounter.Inc()
	return c.String(http.StatusOK, "ok")
}

func logValuesFunc(opt *HttpServerOptions) func(c *echo.Context, v middleware.RequestLoggerValues) (err error) {
	return func(c *echo.Context, v middleware.RequestLoggerValues) (err error) {
		logFn := opt.Log.Error
		msg := "request"

		args := append(make([]any, 0, 6), NAME, opt.Name, "uri", v.URI, "status", v.Status, "latency", v.Latency.String())

		switch {
		case strings.HasPrefix(v.URI, opt.HealthURI):
			fallthrough
		case strings.HasPrefix(v.URI, opt.MetricURI):
			logFn = opt.Log.Debug
		case v.Status < 400:
			logFn = opt.Log.Info
		case v.Status < 500:
			logFn = opt.Log.Warn
			fallthrough
		default:
			args = append(args, "err", v.Error)
		}
		logFn(msg, args...)
		return nil
	}
}
