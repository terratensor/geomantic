package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/config"
)

type NameEntry struct {
	GeohashesString map[string]bool
	GeohashesInt    map[uint64]bool
	FirstSeen       int64
}

type NameDictBuilder struct {
	cfg           *config.Config
	client        *manticore.ManticoreClient
	httpClient    *http.Client
	excludeCJK    bool
	excludeArabic bool
}

func NewNameDictBuilder(cfg *config.Config, client *manticore.ManticoreClient) *NameDictBuilder {
	return &NameDictBuilder{
		cfg:           cfg,
		client:        client,
		httpClient:    &http.Client{Timeout: 60 * time.Second},
		excludeCJK:    cfg.ExcludeCJK,
		excludeArabic: cfg.ExcludeArabic,
	}
}

// BuildDictionary строит словарь всех имён с геохешами
func (b *NameDictBuilder) BuildDictionary(ctx context.Context) error {
	log.Println("Starting name dictionary building...")
	start := time.Now()

	lastID := int64(0)
	limit := 100000
	totalProcessed := 0
	totalInserted := 0
	maxRetries := 3

	// Создаём таблицу если не существует
	if err := b.ensureTable(ctx); err != nil {
		return err
	}

	for {
		var rows []map[string]interface{}
		var err error

		// Пытаемся выполнить запрос с повторными попытками
		for attempt := 0; attempt < maxRetries; attempt++ {
			if attempt > 0 {
				waitTime := time.Second * time.Duration(attempt+1)
				log.Printf("Retry %d for batch after %v", attempt+1, waitTime)
				time.Sleep(waitTime)
			}

			rows, err = b.fetchBatch(ctx, lastID, limit)
			if err == nil {
				break
			}
			log.Printf("Batch failed (attempt %d): %v", attempt+1, err)
		}

		if err != nil {
			return fmt.Errorf("failed after %d attempts at ID %d: %w", maxRetries, lastID, err)
		}

		if len(rows) == 0 {
			log.Println("No more records")
			break
		}

		// Обрабатываем батч в map
		batchMap := b.processBatch(rows)

		// Конвертируем в документы для вставки
		docs := b.convertToDocuments(batchMap)

		// Вставляем в Manticore
		if err := b.client.BulkInsertNames(ctx, docs); err != nil {
			return fmt.Errorf("failed to insert batch: %w", err)
		}

		// Обновляем lastID из последней записи
		if lastRow, ok := rows[len(rows)-1]["id"].(float64); ok {
			lastID = int64(lastRow)
		}

		totalProcessed += len(rows)
		totalInserted += len(docs)

		log.Printf("Processed up to ID %d: %d geonames, %d unique names",
			lastID, totalProcessed, totalInserted)

		// Небольшая задержка между батчами
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("Name dictionary built in %v", time.Since(start))
	log.Printf("Total: %d geonames processed, %d unique names inserted",
		totalProcessed, totalInserted)

	return nil
}

// ensureTable проверяет существование таблицы
func (b *NameDictBuilder) ensureTable(ctx context.Context) error {
	exists, err := b.client.TableExists(ctx, "geoname_dict")
	log.Printf("Table 'geoname_dict' exists: %v", exists)
	if err != nil {
		return err
	}

	if !exists {
		if err := b.client.CreateNameDictTable(ctx); err != nil {
			return err
		}
	}

	return nil
}

// fetchBatch получает батч данных из Manticore
func (b *NameDictBuilder) fetchBatch(ctx context.Context, lastID int64, limit int) ([]map[string]interface{}, error) {

	query := fmt.Sprintf(`
        SELECT 
            id,
            name,
            alternatenames,
            geohash_string,
            geohash_int,
            alternate_names.alternatename
        FROM geonames
        LEFT JOIN alternate_names 
		ON alternate_names.id = geonames.parent_id
        WHERE id > %d
        ORDER BY id ASC
        LIMIT %d`, lastID, limit)

	// log.Printf("Executing query: %s", query)

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
		return nil, fmt.Errorf("HTTP status %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
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

		// Создаём новую map, объединяя _id и _source
		row := make(map[string]interface{})

		// Добавляем id из верхнего уровня
		if id, ok := hitMap["_id"]; ok {
			row["id"] = id
		}

		// Добавляем все поля из _source
		if source, ok := hitMap["_source"].(map[string]interface{}); ok {
			for k, v := range source {
				row[k] = v
			}
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// processBatch обрабатывает батч строк в map уникальных имён
func (b *NameDictBuilder) processBatch(rows []map[string]interface{}) map[string]*NameEntry {
	batchMap := make(map[string]*NameEntry)

	for _, row := range rows {
		// log.Printf("Processing row: %+v", row)

		// Получаем ID из поля id (которое мы добавили из _id)
		idFloat, ok := row["id"].(float64)
		if !ok {
			log.Printf("Warning: no id in row, row: %+v", row)
			continue
		}
		geonameID := int64(idFloat)

		// Получаем строковый геохеш (может быть nil)
		var geohashStr string
		if val, ok := row["geohash_string"]; ok && val != nil {
			geohashStr, _ = val.(string)
		}

		// Получаем числовой геохеш (может быть nil)
		var geohashInt uint64
		if val, ok := row["geohash_int"]; ok && val != nil {
			if ghFloat, ok := val.(float64); ok {
				geohashInt = uint64(ghFloat)
			}
		}

		// Добавляем основное имя
		if name, ok := row["name"].(string); ok && name != "" {
			b.addNameToMap(batchMap, name, geohashStr, geohashInt, geonameID)
		}

		// Парсим alternatenames (через запятую)
		if altNames, ok := row["alternatenames"]; ok && altNames != nil {
			if altStr, ok := altNames.(string); ok && altStr != "" {
				for _, altName := range strings.Split(altStr, ",") {
					altName = strings.TrimSpace(altName)
					if altName != "" {
						b.addNameToMap(batchMap, altName, geohashStr, geohashInt, geonameID)
					}
				}
			}
		}

		// Добавляем из alternate_names
		if altName, ok := row["alternate_names.alternatename"]; ok && altName != nil {
			if altStr, ok := altName.(string); ok && altStr != "" {
				b.addNameToMap(batchMap, altStr, geohashStr, geohashInt, geonameID)
			}
		}
	}

	return batchMap
}

// addNameToMap добавляет имя в map с проверкой исключений
func (b *NameDictBuilder) addNameToMap(
	batchMap map[string]*NameEntry,
	name, geohashStr string,
	geohashInt uint64,
	geonameID int64,
) {
	// Проверяем, нужно ли включать имя
	if !b.shouldInclude(name) {
		return
	}

	entry, exists := batchMap[name]
	if !exists {
		entry = &NameEntry{
			GeohashesString: make(map[string]bool),
			GeohashesInt:    make(map[uint64]bool),
			FirstSeen:       geonameID,
		}
		batchMap[name] = entry
	}

	if geohashStr != "" {
		entry.GeohashesString[geohashStr] = true
	}
	if geohashInt > 0 {
		entry.GeohashesInt[geohashInt] = true
	}
}

// convertToDocuments конвертирует map в документы для Manticore
func (b *NameDictBuilder) convertToDocuments(batchMap map[string]*NameEntry) []map[string]interface{} {
	docs := make([]map[string]interface{}, 0, len(batchMap))

	// Генератор ID (можно использовать хэш от имени)
	id := time.Now().UnixNano()

	for name, entry := range batchMap {
		// Конвертируем map геохешей в slice для multi64
		geohashesInt := make([]uint64, 0, len(entry.GeohashesInt))
		for gh := range entry.GeohashesInt {
			geohashesInt = append(geohashesInt, gh)
		}

		// Конвертируем map в строку через запятую
		geohashesStr := make([]string, 0, len(entry.GeohashesString))
		for gh := range entry.GeohashesString {
			geohashesStr = append(geohashesStr, gh)
		}

		doc := map[string]interface{}{
			"id":               id, // Можно генерировать детерминированный ID из имени
			"name":             name,
			"geohashes_uint64": geohashesInt,
			"geohashes_string": strings.Join(geohashesStr, ","),
			"occurrences":      len(geohashesInt),
			"first_geoname_id": entry.FirstSeen,
		}
		docs = append(docs, doc)
		id++
	}

	return docs
}

// shouldInclude проверяет, нужно ли включать имя в словарь
func (b *NameDictBuilder) shouldInclude(name string) bool {
	if name == "" {
		return false
	}

	for _, r := range name {
		// Проверка CJK
		if b.excludeCJK && isCJKRune(r) {
			return false
		}
		// Проверка арабского
		if b.excludeArabic && isArabicRune(r) {
			return false
		}
	}
	return true
}

// hasCJK проверяет наличие CJK иероглифов в строке
func hasCJK(s string) bool {
	for _, r := range s {
		if isCJKRune(r) {
			return true
		}
	}
	return false
}

// isCJKRune определяет, является ли руна CJK иероглифом
func isCJKRune(r rune) bool {
	// Основные диапазоны CJK
	return (r >= 0x4E00 && r <= 0x9FFF) || // CJK Unified Ideographs
		(r >= 0x3400 && r <= 0x4DBF) || // CJK Unified Ideographs Extension A
		(r >= 0x20000 && r <= 0x2A6DF) || // CJK Unified Ideographs Extension B
		(r >= 0x2A700 && r <= 0x2B73F) || // CJK Unified Ideographs Extension C
		(r >= 0x2B740 && r <= 0x2B81F) || // CJK Unified Ideographs Extension D
		(r >= 0x2B820 && r <= 0x2CEAF) || // CJK Unified Ideographs Extension E
		(r >= 0x2CEB0 && r <= 0x2EBEF) || // CJK Unified Ideographs Extension F
		(r >= 0x3000 && r <= 0x303F) || // CJK Symbols and Punctuation
		(r >= 0x3040 && r <= 0x309F) || // Hiragana
		(r >= 0x30A0 && r <= 0x30FF) || // Katakana
		(r >= 0x31F0 && r <= 0x31FF) || // Katakana Phonetic Extensions
		(r >= 0xFF00 && r <= 0xFFEF) // Halfwidth and Fullwidth Forms
}

// isArabicRune определяет, является ли руна арабским символом
func isArabicRune(r rune) bool {
	// Основной арабский блок
	return (r >= 0x0600 && r <= 0x06FF) || // Arabic
		(r >= 0x0750 && r <= 0x077F) || // Arabic Supplement
		(r >= 0x08A0 && r <= 0x08FF) || // Arabic Extended-A
		(r >= 0x0870 && r <= 0x089F) || // Arabic Extended-B
		(r >= 0xFB50 && r <= 0xFDFF) || // Arabic Presentation Forms-A
		(r >= 0xFE70 && r <= 0xFEFF) // Arabic Presentation Forms-B
}

// isArabicString проверяет, содержит ли строка арабские символы
func isArabicString(s string) bool {
	for _, r := range s {
		if isArabicRune(r) {
			return true
		}
	}
	return false
}
