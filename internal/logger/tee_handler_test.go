package logger

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

// mockHandler нужен для точной проверки вызовов и имитации ошибок
type mockHandler struct {
	enabledFunc   func(ctx context.Context, l slog.Level) bool
	handleFunc    func(ctx context.Context, r slog.Record) error
	withAttrsFunc func(attrs []slog.Attr) slog.Handler
	withGroupFunc func(name string) slog.Handler
}

func (m *mockHandler) Enabled(ctx context.Context, l slog.Level) bool {
	if m.enabledFunc != nil {
		return m.enabledFunc(ctx, l)
	}
	return true
}

func (m *mockHandler) Handle(ctx context.Context, r slog.Record) error {
	if m.handleFunc != nil {
		return m.handleFunc(ctx, r)
	}
	return nil
}

func (m *mockHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if m.withAttrsFunc != nil {
		return m.withAttrsFunc(attrs)
	}
	return m
}

func (m *mockHandler) WithGroup(name string) slog.Handler {
	if m.withGroupFunc != nil {
		return m.withGroupFunc(name)
	}
	return m
}

func TestTeeHandler_NilAndEmptyHandlers(t *testing.T) {
	ctx := context.Background()
	record := slog.NewRecord(time.Now(), slog.LevelInfo, "test", 0)

	// Тест 1: Пустой список хэндлеров не должен паниковать
	thEmpty := &teeHandler{handlers: []slog.Handler{}}
	if thEmpty.Enabled(ctx, slog.LevelInfo) {
		t.Error("Expected Enabled to be false for empty handlers")
	}
	if err := thEmpty.Handle(ctx, record); err != nil {
		t.Errorf("Unexpected error with empty handlers: %v", err)
	}

	// Тест 2: Слайс с nil внутри не должен вызывать panic
	thNil := &teeHandler{handlers: []slog.Handler{nil, nil}}
	if thNil.Enabled(ctx, slog.LevelInfo) {
		t.Error("Expected Enabled to be false for nil handlers")
	}
	if err := thNil.Handle(ctx, record); err != nil {
		t.Errorf("Unexpected error with nil handlers: %v", err)
	}

	// Проверяем, что WithAttrs и WithGroup на nil-структурах тоже работают стабильно
	_ = thNil.WithAttrs([]slog.Attr{slog.String("k", "v")})
	_ = thNil.WithGroup("test_group")
}

func TestTeeHandler_ErrorPropagation(t *testing.T) {
	ctx := context.Background()
	record := slog.NewRecord(time.Now(), slog.LevelInfo, "test", 0)
	expectedErr := errors.New("database log failure")

	// Первый хэндлер работает успешно
	h1 := &mockHandler{
		handleFunc: func(ctx context.Context, r slog.Record) error { return nil },
	}
	// Второй хэндлер возвращает ошибку
	h2 := &mockHandler{
		handleFunc: func(ctx context.Context, r slog.Record) error { return expectedErr },
	}

	th := &teeHandler{handlers: []slog.Handler{h1, h2}}

	// teeHandler должен по цепочке дойти до h2 и вернуть его ошибку
	err := th.Handle(ctx, record)
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

func TestTeeHandler_MethodsImmutability(t *testing.T) {
	// Проверяем, что методы WithAttrs и WithGroup возвращают НОВЫЙ teeHandler,
	// не изменяя оригинальный (принцип иммутабельности)

	h := &mockHandler{}
	originalTee := &teeHandler{handlers: []slog.Handler{h}}

	// Вызываем методы изменения контекста
	modifiedTee1 := originalTee.WithAttrs([]slog.Attr{slog.Int("a", 1)}).(*teeHandler)
	modifiedTee2 := originalTee.WithGroup("g").(*teeHandler)

	if originalTee == modifiedTee1 || originalTee == modifiedTee2 {
		t.Error("WithAttrs and WithGroup must return a new instance of teeHandler, not mutate the original")
	}

	if len(originalTee.handlers) != len(modifiedTee1.handlers) {
		t.Error("Handlers count mismatch during copy")
	}
}
