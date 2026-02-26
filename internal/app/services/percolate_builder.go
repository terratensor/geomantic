package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/config"
)

type PercolateBuilder struct {
	cfg        *config.Config
	client     *manticore.ManticoreClient
	httpClient *http.Client
}

type DictRow struct {
	ID              uint64
	Name            string
	GeohashesString string
}

func NewPercolateBuilder(cfg *config.Config, client *manticore.ManticoreClient) *PercolateBuilder {
	return &PercolateBuilder{
		cfg:        cfg,
		client:     client,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

func (b *PercolateBuilder) Build(ctx context.Context) error {
	log.Println("Starting percolate table build...")
	start := time.Now()

	if err := b.client.CreatePercolateTable(ctx); err != nil {
		return err
	}

	lastID := uint64(0)
	limit := 100000
	maxRetries := 5
	total := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rows, err := b.fetchDictBatch(ctx, lastID, limit, maxRetries)
		if err != nil {
			return err
		}

		if len(rows) == 0 {
			log.Println("No more records")
			break
		}

		// Проверяем уникальность ID в батче
		seen := make(map[uint64]bool)
		uniqueRows := make([]DictRow, 0, len(rows))

		for _, row := range rows {
			if seen[row.ID] {
				log.Printf("WARNING: duplicate ID %d in batch, skipping", row.ID)
				continue
			}
			seen[row.ID] = true
			uniqueRows = append(uniqueRows, row)
		}

		if len(uniqueRows) != len(rows) {
			log.Printf("Removed %d duplicates from batch", len(rows)-len(uniqueRows))
		}

		// Подготавливаем документы - удаляем все спецсимволы
		docs := make([]map[string]interface{}, 0, len(uniqueRows))
		for _, row := range uniqueRows {
			// Удаляем все спецсимволы, оставляем только буквы, цифры и пробелы
			cleanName := cleanQueryText(row.Name)

			doc := map[string]interface{}{
				"id":    row.ID,
				"query": fmt.Sprintf(`"%s"`, cleanName),
				"tags":  row.GeohashesString,
			}
			docs = append(docs, doc)
		}
		// Вставляем батчами
		for i := 0; i < len(docs); i += b.cfg.PercolateBatchSize {
			end := i + b.cfg.PercolateBatchSize
			if end > len(docs) {
				end = len(docs)
			}

			// При ошибке, логируем весь проблемный батч
			if err := b.client.BulkInsertPercolate(ctx, docs[i:end]); err != nil {
				log.Printf("ERROR in batch. First 10 docs:")
				for j, doc := range docs[i:end] {
					if j < 10 {
						log.Printf("  DOC %d: id=%v, query=%v", j, doc["id"], doc["query"])
					}
				}
				return fmt.Errorf("failed to insert batch at offset %d: %w", i, err)
			}
		}

		lastID = uniqueRows[len(uniqueRows)-1].ID
		total += len(uniqueRows)
		log.Printf("Processed %d records, lastID: %d", total, lastID)

		if len(rows) < limit {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("Percolate table built with %d queries in %v", total, time.Since(start))
	return nil
}

func (b *PercolateBuilder) fetchDictBatch(ctx context.Context, lastID uint64, limit int, maxRetries int) ([]DictRow, error) {
	query := fmt.Sprintf(`
        SELECT id, name, geohashes_string
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
			waitTime := time.Second * time.Duration(attempt+1)
			log.Printf("Retry %d for dict batch after %v (lastID: %d)", attempt+1, waitTime, lastID)
			time.Sleep(waitTime)
		}

		rows, err = b.fetchRows(ctx, query)
		if err == nil {
			break
		}

		if strings.Contains(err.Error(), "EOF") {
			log.Printf("Connection lost (attempt %d): %v", attempt+1, err)
			b.httpClient = &http.Client{Timeout: 60 * time.Second}
		} else {
			log.Printf("Batch failed (attempt %d): %v", attempt+1, err)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed after %d attempts at ID %d: %w", maxRetries, lastID, err)
	}

	if len(rows) == 0 {
		return nil, nil
	}

	result := make([]DictRow, 0, len(rows))
	for _, row := range rows {
		idVal, ok := row["id"]
		if !ok || idVal == nil {
			continue
		}

		var id uint64
		switch v := idVal.(type) {
		case float64:
			id = uint64(v)
		case json.Number:
			if i, err := v.Int64(); err == nil {
				id = uint64(i)
			} else {
				continue
			}
		case int64:
			id = uint64(v)
		case uint64:
			id = v
		default:
			continue
		}

		name, ok := row["name"].(string)
		if !ok {
			continue
		}

		tags, ok := row["geohashes_string"].(string)
		if !ok {
			continue
		}

		result = append(result, DictRow{
			ID:              id,
			Name:            name,
			GeohashesString: tags,
		})
	}

	return result, nil
}

func (b *PercolateBuilder) fetchRows(ctx context.Context, query string) ([]map[string]interface{}, error) {
	resp, err := b.httpClient.Post(
		fmt.Sprintf("http://%s:%d/sql", b.cfg.ManticoreHost, b.cfg.ManticorePort),
		"text/plain",
		strings.NewReader(query),
	)
	if err != nil {
		return nil, fmt.Errorf("HTTP error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP status %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	if errVal, ok := result["error"]; ok && errVal != nil {
		return nil, fmt.Errorf("SQL error: %v", errVal)
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
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		row := make(map[string]interface{})

		if idVal, ok := hitMap["_id"]; ok && idVal != nil {
			row["id"] = idVal
		}

		if source, ok := hitMap["_source"].(map[string]interface{}); ok {
			for k, v := range source {
				row[k] = v
			}
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// cleanQueryText удаляет все символы, кроме букв, цифр и пробелов
func cleanQueryText(s string) string {
	return strings.Map(func(r rune) rune {
		// Оставляем буквы, цифры и пробелы
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			return r
		}
		// Всё остальное удаляем
		return -1
	}, s)
}

func escapeManticoreQuery(s string) string {
	specialChars := []string{`!`, `"`, `$`, `'`, `(`, `)`, `/`, `<`, `@`, `\`, `^`, `|`, `~`}
	// Убрали дефис из списка спецсимволов

	for _, ch := range specialChars {
		s = strings.ReplaceAll(s, ch, `\`+ch)
	}
	return s
}
