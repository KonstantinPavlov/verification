package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type TestResources struct {
	Value string
}

func TestContainer_Lifecycle(t *testing.T) {
	t.Parallel() // Разрешаем параллельное выполнение

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Передаем локальный контекст теста в конструктор
	c := NewContainer[TestResources](ctx)
	res := c.GetResources()
	res.Value = "hello"

	var mu sync.Mutex
	var stopOrder []string

	startFn1 := func(ctr Container[TestResources]) StopFn {
		if ctr.GetResources().Value != "hello" {
			t.Errorf("expected resource value 'hello', got '%s'", ctr.GetResources().Value)
		}
		return func() {
			mu.Lock()
			stopOrder = append(stopOrder, "stop1")
			mu.Unlock()
		}
	}

	startFn2 := func(ctr Container[TestResources]) StopFn {
		return func() {
			mu.Lock()
			stopOrder = append(stopOrder, "stop2")
			mu.Unlock()
		}
	}

	expectedErr := errors.New("service shutdown execution")

	var errResult error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		errResult = c.Start(startFn1, startFn2)
	}()

	// Даем горутине запуститься
	time.Sleep(10 * time.Millisecond)

	// Останавливаем контейнер по кастомной причине
	c.Stop(expectedErr)
	wg.Wait()

	// Проверяем кастомную ошибку
	if !errors.Is(errResult, expectedErr) {
		t.Errorf("expected error '%v', got '%v'", expectedErr, errResult)
	}

	// Проверяем обратный порядок хуков остановки
	mu.Lock()
	defer mu.Unlock()
	if len(stopOrder) != 2 {
		t.Fatalf("expected 2 stop hooks executed, got %d", len(stopOrder))
	}
	// ИСПРАВЛЕНО: проверяем каждый элемент слайса по его индексу
	if stopOrder[0] != "stop2" || stopOrder[1] != "stop1" {
		t.Errorf("expected reverse stop order ['stop2', 'stop1'], got %v", stopOrder)
	}

}

func TestContainer_CancelContextDirectly(t *testing.T) {
	t.Parallel() // Тест изолирован и не зависит от других

	ctx, cancel := context.WithCancel(context.Background())

	// Передаем изолированный контекст, который мы отменим вручную из теста
	c := NewContainer[TestResources](ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	var errResult error

	go func() {
		defer wg.Done()
		errResult = c.Start()
	}()

	// Даем горутине запуститься
	time.Sleep(10 * time.Millisecond)

	// Отменяем родительский контекст напрямую
	cancel()
	wg.Wait()

	// Проверяем, что стандартный context.Canceled обрабатывается как nil
	if errResult != nil {
		t.Errorf("expected nil error on standard cancel, got %v", errResult)
	}
}
