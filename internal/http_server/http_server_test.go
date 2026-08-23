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
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// Тестируем дефолтные опции сервера
func TestDefaultHttpServerOptions(t *testing.T) {
	opts := DefaultHttpServerOptions()
	if opts == nil {
		t.Fatal("Expected non-nil options")
	}
	if opts.Name != "default_http_server" {
		t.Errorf("Expected name 'default_http_server', got %q", opts.Name)
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
	c := e.NewContext(req, rec)

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
	// Перехватываем вывод логгера через стандартный буфер
	logBuf := new(bytes.Buffer)
	
	// Используем стандартный TextHandler из библиотеки slog, задав ему уровень DEBUG.
	// Это избавляет нас от вызова неэкспортируемых методов из пакета logger.
	stdHandler := slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	
	// Инициализируем ваш логгер стандартным хэндлером напрямую
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

// Тестируем запуск и остановку сервера через интерфейс HttpServer
func TestServer_StartAndStop(t *testing.T) {
	logBuf := new(bytes.Buffer)
	b := logger.NewDefaultBuilder().LevelStr("INFO")
	customLogger := logger.New(b.Build(logBuf))

	opts := &HttpServerOptions{
		Name:    "lifecycle_server",
		Address: "127.0.0.1:0", // Порт 0 заставит ОС выдать случайный свободный порт
		ConnectedRoutes: func(e *echo.Echo) {
			e.GET("/test", func(c echo.Context) error { return c.String(200, "live") })
		},
		Log:       customLogger,
		HealthURI: "/health",
		MetricURI: "/metrics",
	}

	s := NewHttpServer(opts)
	srvInstance, ok := s.(*server)
	if !ok {
		t.Fatal("Expected type *server from NewHttpServer")
	}

	// Запускаем сервер в отдельной горутине, чтобы он не заблокировал тест
	go func() {
		_ = s.Start()
	}()

	// Даем серверу долю секунды на инициализацию сокета
	time.Sleep(50 * time.Millisecond)

	// Проверяем, что в логи ушло сообщение о старте
	if !strings.Contains(logBuf.String(), "Start http server") {
		t.Errorf("Expected 'Start http server' log message, got: %q", logBuf.String())
	}

	// Останавливаем сервер
	s.Stop()
	time.Sleep(50 * time.Millisecond)

	// Проверяем, что в логи ушло сообщение об остановке
	if !strings.Contains(logBuf.String(), "Stop http server") {
		t.Errorf("Expected 'Stop http server' log message, got: %q", logBuf.String())
	}

	// Убеждаемся, что контекст закрылся
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_ = srvInstance.echo.Shutdown(ctx)
}
