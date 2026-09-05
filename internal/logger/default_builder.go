package logger

import (
	"io"
	"log/slog"
)

type DefaultBuilder interface {
	HandlerBuilder
	Level(l slog.Level) DefaultBuilder
	LevelStr(s string) DefaultBuilder
}

func NewDefaultBuilder() (db DefaultBuilder) {
	return &defaultBuilder{
		level: slog.LevelDebug,
	}
}

type defaultBuilder struct {
	level slog.Level
}

func (db *defaultBuilder) Level(l slog.Level) (u DefaultBuilder) {
	db.level = l
	return db
}

func (db *defaultBuilder) LevelStr(s string) (u DefaultBuilder) {
	db.level = StrLevel(s).Level()
	return db
}

func (db *defaultBuilder) Build(w io.Writer) (handler slog.Handler) {
	return &DefaultHandler{
		commonHandler: newCommonHandler(
			w,
			&slog.HandlerOptions{AddSource: true, Level: db.level},
		),
	}
}
