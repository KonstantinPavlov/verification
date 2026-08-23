package logger

import (
	"context"
	"log/slog"
)

type teeHandler struct {
	handlers []slog.Handler
}

func (th *teeHandler) Enabled(ctx context.Context, level slog.Level) (ok bool) {
	for i := range th.handlers {
		handler := th.handlers[i]
		if handler != nil && handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (th *teeHandler) Handle(ctx context.Context, r slog.Record) (err error) {
	for i := range th.handlers {
		handler := th.handlers[i]
		if handler == nil {
			continue
		}
		if !handler.Enabled(ctx, r.Level) {
			continue
		}
		if e := handler.Handle(ctx, r); e != nil {
			err = e
		}
	}
	return err
}

func (th *teeHandler) WithAttrs(attrs []slog.Attr) (h slog.Handler) {
	new := &teeHandler{
		handlers: make([]slog.Handler, len(th.handlers)),
	}
	for i := range th.handlers {
		handler := th.handlers[i]
		if handler != nil {
			new.handlers[i] = handler.WithAttrs(attrs)
		}
	}
	return new
}

func (th *teeHandler) WithGroup(name string) (h slog.Handler) {
	new := &teeHandler{
		handlers: make([]slog.Handler, len(th.handlers)),
	}
	for i := range th.handlers {
		handler := th.handlers[i]
		if handler != nil {
			new.handlers[i] = handler.WithGroup(name)
		}
	}
	return new
}
