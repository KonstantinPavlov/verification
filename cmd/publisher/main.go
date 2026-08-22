package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	PG_CONN_STR = "postgres://admin:admin123@host.docker.internal:5432/metrics?replication=database"
	SLOT_NAME   = "metric_slot"
)

// Хранилище схем таблиц (ID связи -> Описание колонок)
var relations = make(map[uint32]*pglogrepl.RelationMessage)

func main() {
	ctx := context.Background()

	// 1. Подключение к БД
	connConfig, err := pgconn.ParseConfig(PG_CONN_STR)
	if err != nil {
		log.Fatalf("Ошибка конфигурации PG: %v", err)
	}
	conn, err := pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		log.Fatalf("Ошибка подключения к PG: %v", err)
	}
	defer conn.Close(ctx)

	// 2. Старт репликации
	pluginArgs := []string{"proto_version '1'", "publication_names 'replication_publication'"}
	err = pglogrepl.StartReplication(ctx, conn, SLOT_NAME, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	})
	if err != nil {
		log.Fatalf("Не удалось запустить репликацию: %v", err)
	}
	log.Printf("[СТАРТ] Универсальный слушатель запущен. Мониторим слот '%s'...", SLOT_NAME)

	nextClientAck := time.Now().Add(10 * time.Second)
	var clientXLogPos pglogrepl.LSN

	// 3. Цикл чтения WAL
	for {
		if time.Now().After(nextClientAck) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Printf("Ошибка отправки heartbeat: %v", err)
			}
			nextClientAck = time.Now().Add(10 * time.Second)
		}

		readCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		msg, err := conn.ReceiveMessage(readCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			log.Fatalf("Ошибка чтения сообщения: %v", err)
		}

		switch m := msg.(type) {
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				continue
			}

			switch m.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					log.Fatalf("Ошибка пинга: %v", err)
				}
				if pkm.ReplyRequested {
					nextClientAck = time.Now()
				}

			case pglogrepl.XLogDataByteID:
				xlogMsg, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					log.Fatalf("Ошибка XLogData: %v", err)
				}
				clientXLogPos = xlogMsg.WALStart + pglogrepl.LSN(len(xlogMsg.WALData))

				logicalMsg, err := pglogrepl.Parse(xlogMsg.WALData)
				if err != nil {
					log.Fatalf("Ошибка логического сообщения: %v", err)
				}

				switch lm := logicalMsg.(type) {
				case *pglogrepl.RelationMessage:
					relations[lm.RelationID] = lm

				case *pglogrepl.InsertMessage:
					rel := relations[lm.RelationID]
					if pkInfo, ok := extractPrimaryKey(rel, lm.Tuple); ok {
						log.Printf("[INSERT] Таблица: %s | Ключ: %s", rel.RelationName, pkInfo)
					}

				case *pglogrepl.UpdateMessage:
					rel := relations[lm.RelationID]
					if pkInfo, ok := extractPrimaryKey(rel, lm.NewTuple); ok {
						log.Printf("[UPDATE] Таблица: %s | Ключ: %s", rel.RelationName, pkInfo)
					}

				case *pglogrepl.DeleteMessage:
					rel := relations[lm.RelationID]
					if pkInfo, ok := extractPrimaryKey(rel, lm.OldTuple); ok {
						log.Printf("[DELETE] Таблица: %s | Ключ: %s", rel.RelationName, pkInfo)
					}
				}
			}
		}
	}
}

// УНИВЕРСАЛЬНАЯ функция: ищет колонку с флагом логического первичного ключа (Flags == 1)
// УНИВЕРСАЛЬНАЯ функция для одиночных и композитных ключей
func extractPrimaryKey(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (string, bool) {
	if rel == nil || tuple == nil {
		return "", false
	}

	var parts []string

	// Бежим по всем колонкам, которые описал Postgres для этой таблицы
	for i, col := range rel.Columns {
		// Флаг 1 означает, что колонка входит в состав уникального ключа / Primary Key
		if col.Flags == 1 {
			if i >= len(tuple.Columns) {
				return "", false
			}
			
			colData := tuple.Columns[i]
			if colData.DataType == 't' { // Данные в текстовом формате
				val := string(colData.Data)
				// Формируем кусочек ключа: "имя_колонки=значение"
				parts = append(parts, fmt.Sprintf("%s=%s", col.Name, val))
			}
		}
	}

	// Если не нашли ни одной колонки с флагом ключа
	if len(parts) == 0 {
		return "", false
	}

	// Склеиваем все части составного ключа через запятую
	// На выходе для одиночного ключа будет: "id=10"
	// Для составного ключа будет: "order_id=5,item_id=142"
	return strings.Join(parts, ","), true
}