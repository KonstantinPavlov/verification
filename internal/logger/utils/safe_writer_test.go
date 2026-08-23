package utils

import (
	"bytes"
	"sync"
	"testing"
)

// Проверяем базовую запись и работу с опциональным мьютексом
func TestNewSafeWriter_WithAndWithoutMutex(t *testing.T) {
	buf := new(bytes.Buffer)

	// Вариант 1: Без передачи мьютекса (должен создаться внутренний)
	sw1 := NewSafeWriter(buf)
	if sw1.mu == nil {
		t.Fatal("Expected SafeWriter to initialize internal mutex, but got nil")
	}

	// Вариант 2: С передачей внешнего мьютекса
	customMu := new(sync.Mutex)
	sw2 := NewSafeWriter(buf, customMu)
	if sw2.mu != customMu {
		t.Error("Expected SafeWriter to use the provided custom mutex")
	}
}

// Стресс-тест на конкурентную запись (потокобезопасность)
func TestSafeWriter_Concurrency(t *testing.T) {
	buf := new(bytes.Buffer)
	sw := NewSafeWriter(buf)

	var wg sync.WaitGroup
	goroutinesCount := 100
	iterations := 50
	dataToWrite := []byte("a")

	// Запускаем 100 горутин, которые одновременно пишут в один SafeWriter
	for i := 0; i < goroutinesCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				n, err := sw.Write(dataToWrite)
				if err != nil {
					t.Errorf("Unexpected error during concurrent write: %v", err)
				}
				if n != len(dataToWrite) {
					t.Errorf("Expected to write %d bytes, wrote %d", len(dataToWrite), n)
				}
			}
		}()
	}

	wg.Wait()

	// Проверяем, что записались абсолютно все байты без потерь и повреждений данных
	expectedLength := goroutinesCount * iterations
	if buf.Len() != expectedLength {
		t.Errorf("Data loss detected! Expected buffer length %d, got %d", expectedLength, buf.Len())
	}
}

// Проверяем, что два разных SafeWriter могут синхронизироваться через один мьютекс
func TestSafeWriter_SharedMutex(t *testing.T) {
	buf := new(bytes.Buffer)
	sharedMu := new(sync.Mutex)

	// Создаем два райтера с общим мьютексом
	sw1 := NewSafeWriter(buf, sharedMu)
	sw2 := NewSafeWriter(buf, sharedMu)

	// Проверяем, что они ссылаются на один и тот же объект в памяти
	if sw1.mu != sw2.mu {
		t.Error("SafeWriters do not share the same mutex instance")
	}
}
