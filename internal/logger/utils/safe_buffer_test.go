package utils

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// TestSafeBuffer_Concurrency проверяет потокобезопасность структуры.
// Под флагом -race этот тест упадет, если в методах Write или String будет гонка.
func TestSafeBuffer_Concurrency(t *testing.T) {
	sb := new(SafeBuffer)
	
	const goroutineCount = 100
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutineCount * 2) // Ждем и пишущие, и читающие горутины

	// 1. Запускаем горутины для конкурентной ЗАПИСИ
	for i := range goroutineCount {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				data := fmt.Appendf(nil, "goroutine-%d-data\n", id)
				_, err := sb.Write(data)
				if err != nil {
					t.Errorf("unexpected error on Write: %v", err)
				}
			}
		}(i)
	}

	// 2. Параллельно запускаем горутины для конкурентного ЧТЕНИЯ
	for i := 0; i < goroutineCount; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				// Метод String() вызывается параллельно с записью
				_ = sb.String()
			}
		}()
	}

	// Ожидаем завершения всех потоков
	wg.Wait()

	// 3. Финальная проверка целостности данных
	finalResult := sb.String()
	
	// Проверяем, что данные от первой и последней горутины дошли и не затерлись
	expectedFirst := "goroutine-0-data"
	expectedLast := fmt.Sprintf("goroutine-%d-data", goroutineCount-1)

	if !strings.Contains(finalResult, expectedFirst) {
		t.Errorf("Expected final buffer to contain %q", expectedFirst)
	}
	if !strings.Contains(finalResult, expectedLast) {
		t.Errorf("Expected final buffer to contain %q", expectedLast)
	}
}

// TestSafeBuffer_Basic функциональный тест на базовую работу буфера
func TestSafeBuffer_Basic(t *testing.T) {
	sb := new(SafeBuffer)
	
	n, err := sb.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("Write failed: n=%d, err=%v", n, err)
	}
	
	if sb.String() != "hello" {
		t.Errorf("Expected 'hello', got %q", sb.String())
	}
}