package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/config"
)

func main() {
	// Загружаем конфигурацию
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Создаём Manticore клиент
	client, err := manticore.NewClient(cfg.ManticoreHost, cfg.ManticorePort)
	if err != nil {
		log.Fatalf("Failed to create manticore client: %v", err)
	}

	ctx := context.Background()

	// Список таблиц для удаления (в обратном порядке из-за зависимостей)
	tables := []string{
		"hierarchy_paths",
		"admin_codes",
		"hierarchy",
		"alternate_names",
		"geonames",
	}

	log.Println("Dropping existing tables...")

	for _, table := range tables {
		// Проверяем существует ли таблица
		exists, err := client.TableExists(ctx, table)
		if err != nil {
			log.Printf("Warning: failed to check table %s: %v", table, err)
			continue
		}

		if !exists {
			log.Printf("Table %s does not exist, skipping", table)
			continue
		}

		// Удаляем таблицу через UtilsAPI
		log.Printf("Dropping table %s...", table)

		sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
		req := client.GetClient().UtilsAPI.Sql(ctx).Body(sql).RawResponse(true)

		resp, httpResp, err := client.GetClient().UtilsAPI.SqlExecute(req)
		if err != nil {
			log.Printf("Error dropping table %s: %v", table, err)
			continue
		}

		if httpResp != nil && httpResp.StatusCode != 200 {
			log.Printf("Warning: drop table %s returned HTTP %d", table, httpResp.StatusCode)
			if resp != nil && resp.SqlObjResponse != nil {
				hits := resp.SqlObjResponse.GetHits()
				if error, ok := hits["error"]; ok && error != nil {
					log.Printf("Error details: %v", error)
				}
			}
		} else {
			log.Printf("Table %s dropped successfully", table)
		}

		// Небольшая задержка между операциями
		time.Sleep(200 * time.Millisecond)
	}

	log.Println("All tables dropped successfully!")
	log.Println("You can now run 'make import' to rebuild everything with geohashes")
}
