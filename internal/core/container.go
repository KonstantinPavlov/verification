package core

import (
	"context"
	"errors"
)

type container[T any] struct {

	// Ресурсы контейнера
	resources T
	// Контекст
	ctx    context.Context
	cancel context.CancelCauseFunc
	// Список функций остановки ресусрсов контейнера
	stopHooks []StopFn
}

// Получение контектса контейнера
func (c *container[T]) GetContext() (ctx context.Context) {
	return c.ctx
}

// Полученеи ресурсов контейнера
func (c *container[T]) GetResources() (res *T) {
	return &c.resources
}

func (c *container[T]) Start(fns ...StartFn[T]) (err error) {

	// Запоминаем все функции остановки от ресурсов контейнера
	c.stopHooks = make([]StopFn, 0, len(fns))
init:
	for _, fn := range fns {
		select {
		case <-c.ctx.Done():
			break init
		default:
			c.stopHooks = append(c.stopHooks, fn(c))
		}
	}
	// Останвлиаем контейнер
	<-c.ctx.Done()
	c.stop()
	// Смотрим причну остановки
	err = context.Cause(c.ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (c *container[T]) stop() {
	// Остановка ресурсов в обратном порядке
	for i := len(c.stopHooks) - 1; i >= 0; i-- {
		if fn := c.stopHooks[i]; fn != nil {
			fn()
		}
	}
}

func (c *container[T]) Stop(reason error) {
	c.cancel(reason)
}
