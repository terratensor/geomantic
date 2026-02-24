package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/terratensor/geomantic/internal/adapters/exporters"
	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/app/services/export"
	"github.com/terratensor/geomantic/internal/config"
	"github.com/terratensor/geomantic/internal/core/ports"
)

type ExportService struct {
	client        *manticore.ManticoreClient
	cfg           *config.Config
	writerFactory *exporters.WriterFactory
	httpClient    *http.Client
}

func NewExportService(cfg *config.Config, client *manticore.ManticoreClient) *ExportService {
	return &ExportService{
		client:        client,
		cfg:           cfg,
		writerFactory: exporters.NewWriterFactory(),
		httpClient:    &http.Client{Timeout: 60 * time.Second},
	}
}

func (s *ExportService) ExportGeonameDict(ctx context.Context, options ports.ExportOptions) error {
	log.Printf("Starting export to %s: %s", options.Format, options.FilePath)
	start := time.Now()

	// Создаем writer
	writer, err := s.writerFactory.CreateFileWriter(options.FilePath, options)
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	defer writer.Close()

	// Пишем заголовок
	if err := writer.WriteHeader(export.GeonameDictColumns); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Потоковое чтение и запись
	recordCh, errCh := s.streamRecords(ctx)

	recordCount := 0
	for record := range recordCh {
		if err := writer.WriteRecord(record); err != nil {
			return fmt.Errorf("failed to write record at %d: %w", recordCount, err)
		}
		recordCount++

		if recordCount%100000 == 0 {
			log.Printf("Exported %d records...", recordCount)
		}
	}

	// Проверяем ошибки из канала
	if err := <-errCh; err != nil {
		return fmt.Errorf("error during streaming: %w", err)
	}

	log.Printf("Export completed: %d records in %v", recordCount, time.Since(start))
	return nil
}

// streamRecords читает записи из Manticore и отправляет в канал
func (s *ExportService) streamRecords(ctx context.Context) (<-chan map[string]interface{}, <-chan error) {
	out := make(chan map[string]interface{})
	errCh := make(chan error, 1)

	go func() {
		defer close(out)

		lastID := int64(0)
		limit := 10000
		maxRetries := 3

		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}

			query := fmt.Sprintf(`
                SELECT id, name, geohashes_string, geohashes_uint64, occurrences, first_geoname_id
                FROM geoname_dict
                WHERE id > %d
                ORDER BY id ASC
                LIMIT %d
                OPTION max_matches=%d
            `, lastID, limit, limit)

			var rows []map[string]interface{}
			var err error

			for attempt := 0; attempt < maxRetries; attempt++ {
				if attempt > 0 {
					time.Sleep(time.Second * time.Duration(attempt+1))
				}

				rows, err = s.fetchRows(ctx, query)
				if err == nil {
					break
				}
			}

			if err != nil {
				errCh <- fmt.Errorf("failed to fetch batch at ID %d: %w", lastID, err)
				return
			}

			if len(rows) == 0 {
				break
			}

			for _, row := range rows {
				// Преобразуем id для следующей итерации
				if idVal, ok := row["id"]; ok {
					if idFloat, ok := idVal.(float64); ok {
						lastID = int64(idFloat)
					}
				}
				out <- row
			}

			if len(rows) < limit {
				break
			}
		}

		errCh <- nil
	}()

	return out, errCh
}

// fetchRows выполняет SQL запрос и возвращает строки
func (s *ExportService) fetchRows(ctx context.Context, query string) ([]map[string]interface{}, error) {
	resp, err := s.httpClient.Post(
		fmt.Sprintf("http://%s:%d/sql", s.cfg.ManticoreHost, s.cfg.ManticorePort),
		"text/plain",
		strings.NewReader(query),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil, nil
	}

	hitsList, ok := hits["hits"].([]interface{})
	if !ok {
		return nil, nil
	}

	rows := make([]map[string]interface{}, 0, len(hitsList))
	for _, hit := range hitsList {
		hitMap := hit.(map[string]interface{})

		row := make(map[string]interface{})

		// ID из верхнего уровня
		if id, ok := hitMap["_id"]; ok {
			row["id"] = id
		}

		// Данные из _source
		if source, ok := hitMap["_source"].(map[string]interface{}); ok {
			for k, v := range source {
				row[k] = v
			}
		}

		rows = append(rows, row)
	}

	return rows, nil
}
