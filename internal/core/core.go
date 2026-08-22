package core

import (
	"context"
)

// Функция остановки ресурса
type StopFn func()

// Функция старта ресурса - должна быть не блокирующей!
type StartFn[T any] func(Container[T]) (stop StopFn)

type Container[T any] interface {
	GetContext() (ctx context.Context)

	GetResources() (res *T)

	Start(fns ...StartFn[T]) (err error)

	Stop(reason error)
}

func NewContainer[T any](ctx context.Context) Container[T] {
	ctx, cancel := context.WithCancelCause(ctx)
	return &container[T]{ctx: ctx, cancel: cancel}
}
