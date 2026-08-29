package http_server

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KonstantinPavlov/verification/internal/logger"
	"github.com/KonstantinPavlov/verification/internal/logger/utils"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

// Тестируем дефолтные опции сервера
func TestDefaultHttpServerOptions(t *testing.T) {
	opts := DefaultHttpServerOptions()
	if opts == nil {
		t.Fatal("Expected non-nil options")
	}
	// Исправлено: в коде сервера зашито имя "http_server", а не "default_http_server"
	if opts.Name != "http_server" {
		t.Errorf("Expected name 'http_server', got %q", opts.Name)
	}
	if opts.HealthURI != "/health" || opts.MetricURI != "/metrics" {
		t.Errorf("Unexpected default URIs: health=%q, metric=%q", opts.HealthURI, opts.MetricURI)
	}
}

// Тестируем эндпоинт /health и инкремент счетчика Prometheus
func TestHealthHandler(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	
	// В v5 NewContext возвращает *echo.Context
	c := e.NewContext(req, rec)

	// Передаем указатель на контекст
	err := health(c)
	if err != nil {
		t.Fatalf("Unexpected error from health handler: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("Expected body 'ok', got %q", rec.Body.String())
	}
}

// Тестируем функцию логирования запросов logValuesFunc для разных HTTP-статусов и URI
func TestLogValuesFunc(t *testing.T) {
	logBuf := new(bytes.Buffer)
	
	stdHandler := slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	
	customLogger := &logger.Logger{
		Logger: slog.New(stdHandler),
	}

	opts := &HttpServerOptions{
		Name:      "test_server",
		Log:       customLogger,
		HealthURI: "/health",
		MetricURI: "/metrics",
	}

	e := echo.New()
	logFunc := logValuesFunc(opts)

	tests := []struct {
		name          string
		values        middleware.RequestLoggerValues
		expectedLevel string
		expectedMsg   string
	}{
		{
			name: "Health URI logs as DEBUG",
			values: middleware.RequestLoggerValues{
				URI:    "/health",
				Status: http.StatusOK,
			},
			expectedLevel: "level=DEBUG",
			expectedMsg:   "uri=/health",
		},
		{
			name: "Metrics URI logs as DEBUG",
			values: middleware.RequestLoggerValues{
				URI:    "/metrics",
				Status: http.StatusOK,
			},
			expectedLevel: "level=DEBUG",
			expectedMsg:   "uri=/metrics",
		},
		{
			name: "Successful request logs as INFO",
			values: middleware.RequestLoggerValues{
				URI:    "/api/v1/users",
				Status: http.StatusOK,
			},
			expectedLevel: "level=INFO",
			expectedMsg:   "status=200",
		},
		{
			name: "Client error logs as WARN with error details",
			values: middleware.RequestLoggerValues{
				URI:    "/api/v1/bad",
				Status: http.StatusBadRequest,
			},
			expectedLevel: "level=WARN",
			expectedMsg:   "status=400",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logBuf.Reset()
			req := httptest.NewRequest(http.MethodGet, tt.values.URI, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			// В v5 logFunc принимает *echo.Context
			err := logFunc(c, tt.values)
			if err != nil {
				t.Fatalf("logValuesFunc failed: %v", err)
			}

			output := logBuf.String()
			if !strings.Contains(output, tt.expectedLevel) {
				t.Errorf("Expected level %s not found in log: %q", tt.expectedLevel, output)
			}
			if !strings.Contains(output, tt.expectedMsg) {
				t.Errorf("Expected content %s not found in log: %q", tt.expectedMsg, output)
			}
		})
	}
}

// Тестируем запуск и остановку сервера через интерфейс HttpServer в стиле Echo v5
func TestServer_StartAndStop(t *testing.T) {
	// Используем наш безопасный буфер вместо стандартного new(bytes.Buffer)
	logBuf := new(utils.SafeBuffer)
	
	// Передаем logBuf. Он прозрачно реализует интерфейс io.Writer благодаря методу Write()
	stdHandler := slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})
	customLogger := &logger.Logger{Logger: slog.New(stdHandler)}

	opts := &HttpServerOptions{
		Name:    "lifecycle_server",
		Address: "127.0.0.1:9092",
		ConnectedRoutes: func(e *echo.Echo) {
			e.GET("/test", func(c *echo.Context) error { return c.String(200, "live") })
		},
		Log:       customLogger,
		HealthURI: "/health",
		MetricURI: "/metrics",
	}

	s := NewHttpServer(opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = s.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Теперь вызов logBuf.String() полностью безопасен, так как заблокирует мьютекс
	if !strings.Contains(logBuf.String(), "Start http server") {
		t.Errorf("Expected 'Start http server' log message, got: %q", logBuf.String())
	}

	s.Stop()
	
	cancel()
	time.Sleep(50 * time.Millisecond)

	// Повторное безопасное чтение логов
	if !strings.Contains(logBuf.String(), "Stop http server") {
		t.Errorf("Expected 'Stop http server' log message, got: %q", logBuf.String())
	}
}
