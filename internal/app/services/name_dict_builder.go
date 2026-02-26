package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/config"
)

type NameEntry struct {
	OriginalName    string // Оригинальное имя для вывода
	GeohashesString map[string]bool
	GeohashesInt    map[uint64]bool
	FirstSeen       int64
}

type GeoNameInfo struct {
	ID             int64
	Name           string
	AlternateNames []string
	GeohashString  string
	GeohashInt     uint64
}

type AltNameInfo struct {
	GeonameID     int64
	AlternateName string
}

type NameDictBuilder struct {
	cfg            *config.Config
	client         *manticore.ManticoreClient
	httpClient     *http.Client
	excludeCJK     bool
	excludeArabic  bool
	normalizeNames bool
}

func NewNameDictBuilder(cfg *config.Config, client *manticore.ManticoreClient) *NameDictBuilder {
	return &NameDictBuilder{
		cfg:            cfg,
		client:         client,
		httpClient:     &http.Client{Timeout: 60 * time.Second},
		excludeCJK:     cfg.ExcludeCJK,
		excludeArabic:  cfg.ExcludeArabic,
		normalizeNames: cfg.NormalizeNames,
	}
}

// BuildDictionary строит словарь через сканирование отдельных таблиц
func (b *NameDictBuilder) BuildDictionary(ctx context.Context) error {
	log.Println("Starting name dictionary building with in-memory join...")
	start := time.Now()

	// Шаг 1: Создаём таблицу
	if err := b.ensureTable(ctx); err != nil {
		return err
	}

	// Шаг 2: Загружаем все geonames
	log.Println("Loading geonames...")
	geonames, err := b.loadAllGeonames(ctx)
	if err != nil {
		return fmt.Errorf("failed to load geonames: %w", err)
	}
	log.Printf("Loaded %d geonames", len(geonames))

	// Шаг 3: Загружаем все alternate_names и группируем по geonameid
	log.Println("Loading alternate names...")
	altNamesMap, err := b.loadAllAlternateNames(ctx)
	if err != nil {
		return fmt.Errorf("failed to load alternate names: %w", err)
	}
	log.Printf("Loaded alternate names for %d geonames", len(altNamesMap))

	// Шаг 4: Объединяем и строим словарь
	log.Println("Building name dictionary...")
	nameMap := make(map[string]*NameEntry)

	for _, geo := range geonames {
		// Добавляем основное имя
		b.addGeoNameToMap(nameMap, geo.Name, geo.GeohashString, geo.GeohashInt, geo.ID)

		// Добавляем из alternatenames
		for _, altName := range geo.AlternateNames {
			b.addGeoNameToMap(nameMap, altName, geo.GeohashString, geo.GeohashInt, geo.ID)
		}

		// Добавляем из alternate_names
		if altList, ok := altNamesMap[geo.ID]; ok {
			for _, alt := range altList {
				b.addGeoNameToMap(nameMap, alt.AlternateName, geo.GeohashString, geo.GeohashInt, geo.ID)
			}
		}
	}

	log.Printf("Built %d unique names", len(nameMap))

	// Шаг 5: Сохраняем батчами
	if err := b.saveNameMap(ctx, nameMap); err != nil {
		return fmt.Errorf("failed to save name map: %w", err)
	}

	log.Printf("Name dictionary built in %v", time.Since(start))
	return nil
}

// loadAllGeonames загружает записи из geonames
func (b *NameDictBuilder) loadAllGeonames(ctx context.Context) ([]*GeoNameInfo, error) {
	var result []*GeoNameInfo
	lastID := int64(0)
	limit := 100000
	maxRetries := 5
	testMode := false     // Временно для тестирования
	maxRecords := 1000000 // Ограничение для теста

	// Получаем общее количество для информации
	if testMode {
		log.Printf("TEST MODE: will load only %d records", maxRecords)
	}

	for {
		if testMode && len(result) >= maxRecords {
			log.Printf("Reached test limit of %d records, stopping", maxRecords)
			break
		}

		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		query := fmt.Sprintf(`
            SELECT id, name, alternatenames, geohash_string, geohash_int
            FROM geonames
            WHERE id > %d			
            ORDER BY id ASC
            LIMIT %d
			OPTION max_matches=%d
			`, lastID, limit, limit)

		var rows []map[string]interface{}
		var err error

		// Пытаемся выполнить запрос с повторными попытками
		retrySuccess := false
		for attempt := 0; attempt < maxRetries; attempt++ {
			if attempt > 0 {
				waitTime := time.Second * time.Duration(attempt+1)
				log.Printf("Retry %d for geonames batch after %v (lastID: %d)",
					attempt+1, waitTime, lastID)
				time.Sleep(waitTime)
			}

			rows, err = b.fetchRows(ctx, query)
			if err == nil {
				retrySuccess = true
				break
			}

			if strings.Contains(err.Error(), "EOF") {
				log.Printf("Connection lost (attempt %d): %v", attempt+1, err)
				b.httpClient = &http.Client{Timeout: 60 * time.Second}
			} else {
				log.Printf("Batch failed (attempt %d): %v", attempt+1, err)
			}
		}

		if !retrySuccess {
			return result, fmt.Errorf("failed after %d attempts at ID %d: %w",
				maxRetries, lastID, err)
		}

		if len(rows) == 0 {
			log.Println("No more geonames")
			break
		}

		batchLoaded := 0
		for _, row := range rows {
			// Проверка лимита теста
			if testMode && len(result) >= maxRecords {
				break
			}

			// Безопасное извлечение ID
			idVal, ok := row["id"]
			if !ok || idVal == nil {
				continue
			}
			idFloat, ok := idVal.(float64)
			if !ok {
				continue
			}

			// Безопасное извлечение name
			nameVal, ok := row["name"]
			if !ok || nameVal == nil {
				continue
			}
			name, ok := nameVal.(string)
			if !ok {
				continue
			}

			geo := &GeoNameInfo{
				ID:            int64(idFloat),
				Name:          name,
				GeohashString: safeString(row["geohash_string"]),
				GeohashInt:    safeUint64(row["geohash_int"]),
			}

			// Безопасное извлечение alternatenames
			if altVal, ok := row["alternatenames"]; ok && altVal != nil {
				if altStr, ok := altVal.(string); ok && altStr != "" {
					parts := strings.Split(altStr, ",")
					for _, part := range parts {
						trimmed := strings.TrimSpace(part)
						if trimmed != "" {
							geo.AlternateNames = append(geo.AlternateNames, trimmed)
						}
					}
				}
			}

			result = append(result, geo)
			lastID = geo.ID
			batchLoaded++
		}

		log.Printf("Loaded %d geonames (total: %d, lastID: %d)...",
			batchLoaded, len(result), lastID)

		if len(rows) < limit {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	return result, nil
}

// loadAllAlternateNames загружает все alternate_names и группирует по geonameid
func (b *NameDictBuilder) loadAllAlternateNames(ctx context.Context) (map[int64][]*AltNameInfo, error) {
	result := make(map[int64][]*AltNameInfo)
	lastID := int64(0)
	limit := 100000
	maxRetries := 5
	testMode := false
	maxRecords := 1000000

	// Для отслеживания, сколько уникальных geonameid мы уже обработали
	processedGeonameIDs := make(map[int64]bool)

	for {
		if testMode && len(processedGeonameIDs) >= maxRecords {
			log.Printf("Reached test limit of %d unique geonameid, stopping", maxRecords)
			break
		}

		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		query := fmt.Sprintf(`
            SELECT id, geonameid, alternatename
            FROM alternate_names
            WHERE id > %d
            ORDER BY id ASC
            LIMIT %d
			OPTION max_matches=%d
			`, lastID, limit, limit)

		var rows []map[string]interface{}
		var err error

		retrySuccess := false
		for attempt := 0; attempt < maxRetries; attempt++ {
			if attempt > 0 {
				waitTime := time.Second * time.Duration(attempt+1)
				log.Printf("Retry %d for alternate_names batch after %v (lastID: %d)",
					attempt+1, waitTime, lastID)
				time.Sleep(waitTime)
			}

			rows, err = b.fetchRows(ctx, query)
			if err == nil {
				retrySuccess = true
				break
			}

			if strings.Contains(err.Error(), "EOF") {
				log.Printf("Connection lost (attempt %d): %v", attempt+1, err)
				b.httpClient = &http.Client{Timeout: 60 * time.Second}
			} else {
				log.Printf("Batch failed (attempt %d): %v", attempt+1, err)
			}
		}

		if !retrySuccess {
			return result, fmt.Errorf("failed after %d attempts at ID %d: %w",
				maxRetries, lastID, err)
		}

		if len(rows) == 0 {
			log.Println("No more alternate names")
			break
		}

		batchLoaded := 0
		for _, row := range rows {
			// Проверка лимита теста
			if testMode && len(processedGeonameIDs) >= maxRecords {
				break
			}

			idVal, ok := row["id"]
			if !ok || idVal == nil {
				continue
			}
			idFloat, ok := idVal.(float64)
			if !ok {
				continue
			}

			geonameidVal, ok := row["geonameid"]
			if !ok || geonameidVal == nil {
				continue
			}
			geonameidFloat, ok := geonameidVal.(float64)
			if !ok {
				continue
			}
			geonameID := int64(geonameidFloat)

			altNameVal, ok := row["alternatename"]
			if !ok || altNameVal == nil {
				continue
			}
			altName, ok := altNameVal.(string)
			if !ok || altName == "" {
				continue
			}

			alt := &AltNameInfo{
				GeonameID:     geonameID,
				AlternateName: altName,
			}

			result[geonameID] = append(result[geonameID], alt)
			processedGeonameIDs[geonameID] = true
			lastID = int64(idFloat)
			batchLoaded++
		}

		log.Printf("Loaded %d alternate names (total unique geonameid: %d, lastID: %d)...",
			batchLoaded, len(processedGeonameIDs), lastID)

		if len(rows) < limit {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	return result, nil
}

// fetchRows выполняет SQL запрос и возвращает слайс строк
func (b *NameDictBuilder) fetchRows(ctx context.Context, query string) ([]map[string]interface{}, error) {
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
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP status %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	// Проверяем наличие ошибки в ответе
	if errVal, ok := result["error"]; ok && errVal != nil {
		return nil, fmt.Errorf("SQL error: %v", errVal)
	}

	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		log.Printf("No hits in response: %+v", result)
		return nil, nil
	}

	hitsList, ok := hits["hits"].([]interface{})
	if !ok {
		log.Printf("hits.hits is not []interface{}: %T", hits["hits"])
		return nil, nil
	}

	rows := make([]map[string]interface{}, 0, len(hitsList))
	for _, hit := range hitsList {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		// Создаём новую строку, объединяя _id и _source
		row := make(map[string]interface{})

		// Добавляем id из верхнего уровня
		if idVal, ok := hitMap["_id"]; ok && idVal != nil {
			row["id"] = idVal
		}

		// Добавляем все поля из _source
		if source, ok := hitMap["_source"].(map[string]interface{}); ok {
			for k, v := range source {
				row[k] = v
			}
		}

		rows = append(rows, row)
	}

	// log.Printf("Fetched %d rows", len(rows))
	// if len(rows) > 0 {
	// 	log.Printf("Sample row: %+v", rows[0])
	// }

	return rows, nil
}

// addGeoNameToMap добавляет имя в map с проверкой исключений
func (b *NameDictBuilder) addGeoNameToMap(
	nameMap map[string]*NameEntry,
	name, geohashStr string,
	geohashInt uint64,
	geonameID int64,
) {
	if !b.shouldInclude(name) {
		if strings.ContainsAny(name, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
			// log.Printf("WARNING: Latin name '%s' rejected by shouldInclude", name)
		}
		return
	}

	// normalizedKey := normalizeName(name)
	normalizedKey := name

	entry, exists := nameMap[normalizedKey]
	if !exists {
		entry = &NameEntry{
			OriginalName:    name,
			GeohashesString: make(map[string]bool),
			GeohashesInt:    make(map[uint64]bool),
			FirstSeen:       geonameID,
		}
		nameMap[normalizedKey] = entry
	}

	if geohashStr != "" {
		entry.GeohashesString[geohashStr] = true
	}
	if geohashInt > 0 {
		entry.GeohashesInt[geohashInt] = true
	}
}

// saveNameMap сохраняет nameMap в Manticore батчами
func (b *NameDictBuilder) saveNameMap(ctx context.Context, nameMap map[string]*NameEntry) error {
	batchSize := 10000
	batch := make([]map[string]interface{}, 0, batchSize)
	id := time.Now().UnixNano()

	problemDoc := 0
	for normalizedKey, entry := range nameMap {
		// Пропускаем если нет геохешей
		if len(entry.GeohashesInt) == 0 {
			log.Printf("DEBUG: No geohashes for '%s' (original: '%s')",
				normalizedKey, entry.OriginalName)
			continue
		}

		// Конвертируем map геохешей в slice
		geohashesInt := make([]uint64, 0, len(entry.GeohashesInt))
		for gh := range entry.GeohashesInt {
			if gh > 0 {
				geohashesInt = append(geohashesInt, gh)
			}
		}

		// Проверка: точно ли это массив?
		if len(geohashesInt) == 0 {
			log.Printf("DEBUG: Empty geohashes after conversion for '%s'", entry.OriginalName)
			continue
		}

		// Дополнительная проверка на одиночное значение
		if len(geohashesInt) == 1 {
			// Для одного элемента это всё ещё должен быть массив
			// log.Printf("DEBUG: Single geohash for '%s': %d", entry.OriginalName, geohashesInt[0])
		}

		// Сортируем
		sort.Slice(geohashesInt, func(i, j int) bool { return geohashesInt[i] < geohashesInt[j] })

		// Убираем дубликаты
		uniqueGeo := make([]uint64, 0, len(geohashesInt))
		seen := make(map[uint64]bool)
		for _, gh := range geohashesInt {
			if !seen[gh] {
				seen[gh] = true
				uniqueGeo = append(uniqueGeo, gh)
			}
		}

		geohashesStr := make([]string, 0, len(entry.GeohashesString))
		for gh := range entry.GeohashesString {
			if gh != "" {
				geohashesStr = append(geohashesStr, gh)
			}
		}
		sort.Strings(geohashesStr)

		// Уникализация строк
		uniqueStr := make([]string, 0, len(geohashesStr))
		seenStr := make(map[string]bool)
		for _, gh := range geohashesStr {
			if !seenStr[gh] {
				seenStr[gh] = true
				uniqueStr = append(uniqueStr, gh)
			}
		}

		// mvaArray := make([]interface{}, len(uniqueGeo))
		// for i, v := range uniqueGeo {
		// 	mvaArray[i] = v
		// }

		geohashesInt64 := make([]int64, len(uniqueGeo))
		for i, v := range uniqueGeo {
			geohashesInt64[i] = int64(v)
		}

		doc := map[string]interface{}{
			"id":               id,
			"name":             entry.OriginalName,
			"geohashes_uint64": geohashesInt64,
			"geohashes_string": strings.Join(uniqueStr, ","),
			"occurrences":      len(uniqueGeo),
			"first_geoname_id": entry.FirstSeen,
		}

		// Проверим этот конкретный документ на всякий случай
		problemDoc++
		if problemDoc <= 5 {
			log.Printf("DEBUG Doc %d: %+v", problemDoc, doc)
		}

		batch = append(batch, doc)
		id++

		if len(batch) >= batchSize {
			log.Printf("Inserting batch of %d documents...", len(batch))
			if err := b.client.BulkInsertNames(ctx, batch); err != nil {
				// При ошибке покажем проблемные документы
				// for i, badDoc := range batch {
				// 	log.Printf("Problem doc %d in batch: %+v", i, badDoc)
				// }
				return err
			}
			batch = batch[:0]
		}
	}

	// Финальный батч
	if len(batch) > 0 {
		log.Printf("Inserting final batch of %d documents...", len(batch))
		if err := b.client.BulkInsertNames(ctx, batch); err != nil {
			for i, badDoc := range batch {
				log.Printf("Problem doc %d in final batch: %+v", i, badDoc)
			}
			return err
		}
	}

	return nil
}

// Вспомогательные функции
func safeString(val interface{}) string {
	if val == nil {
		return ""
	}
	if s, ok := val.(string); ok {
		return s
	}
	return ""
}

func safeUint64(val interface{}) uint64 {
	if val == nil {
		return 0
	}
	if f, ok := val.(float64); ok {
		return uint64(f)
	}
	return 0
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
			asciiname,
			alternatenames,
			geohash_string,
			geohash_int,
			alternate_names.alternatename
		FROM geonames
		LEFT JOIN alternate_names 
		ON geonames.id = alternate_names.geonameid
        WHERE id > %d
		GROUP BY asciiname
        ORDER BY id ASC
        LIMIT %d`, lastID, limit)

	log.Printf("Executing query: %s", query)

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

	// Группируем строки по geoname ID
	geoGroups := make(map[int64][]map[string]interface{})

	for _, row := range rows {
		idFloat, ok := row["id"].(float64)
		if !ok {
			continue
		}
		geonameID := int64(idFloat)
		geoGroups[geonameID] = append(geoGroups[geonameID], row)
	}

	// Обрабатываем каждую группу (один геоним со всеми его альтернативными именами)
	for geonameID, group := range geoGroups {
		if len(group) == 0 {
			continue
		}

		// Берем основную информацию из первой записи группы
		firstRow := group[0]

		// Получаем геохеши (ОДИНАКОВЫЕ для всех имён этого геонима)
		var geohashStr string
		if val, ok := firstRow["geohash_string"]; ok && val != nil {
			geohashStr, _ = val.(string)
		}

		var geohashInt uint64
		if val, ok := firstRow["geohash_int"]; ok && val != nil {
			if ghFloat, ok := val.(float64); ok {
				geohashInt = uint64(ghFloat)
			}
		}

		// Добавляем основное имя
		if name, ok := firstRow["name"].(string); ok && name != "" {
			b.addNameToMap(batchMap, name, geohashStr, geohashInt, geonameID)
		}

		// Добавляем имена из alternatenames (через запятую)
		if altNames, ok := firstRow["alternatenames"]; ok && altNames != nil {
			if altStr, ok := altNames.(string); ok && altStr != "" {
				for _, altName := range strings.Split(altStr, ",") {
					altName = strings.TrimSpace(altName)
					if altName != "" {
						b.addNameToMap(batchMap, altName, geohashStr, geohashInt, geonameID)
					}
				}
			}
		}

		// Добавляем имена из alternate_names (все строки группы)
		for _, row := range group {
			if altName, ok := row["alt_name"]; ok && altName != nil {
				if altStr, ok := altName.(string); ok && altStr != "" {
					b.addNameToMap(batchMap, altStr, geohashStr, geohashInt, geonameID)
				}
			}
		}
	}

	return batchMap
}

// normalizeName нормализует имя для использования в качестве ключа
func normalizeName(name string) string {
	// Удаляем лишние пробелы
	name = strings.TrimSpace(name)
	// Приводим к нижнему регистру для сравнения
	name = strings.ToLower(name)
	// Можно добавить другие нормализации при необходимости
	return name
}

// addNameToMap добавляет имя в map с нормализацией
func (b *NameDictBuilder) addNameToMap(
	batchMap map[string]*NameEntry,
	name, geohashStr string,
	geohashInt uint64,
	geonameID int64,
) {
	// Проверяем, нужно ли включать имя
	if !b.shouldInclude(name) {
		if strings.ContainsAny(name, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
			log.Printf("WARNING: Latin name '%s' rejected by shouldInclude", name)
		}
		return
	}

	normalizedKey := name
	// Затем, если включено, применяем нормализацию диакритики
	if b.normalizeNames {
		normalizedKey = normalizeDiacritics(normalizedKey)
	}
	// Нормализуем имя для ключа
	normalizedKey = normalizeName(name)

	entry, exists := batchMap[normalizedKey]
	if !exists {
		entry = &NameEntry{
			OriginalName:    name, // Сохраняем оригинальное имя для вывода
			GeohashesString: make(map[string]bool),
			GeohashesInt:    make(map[uint64]bool),
			FirstSeen:       geonameID,
		}
		batchMap[normalizedKey] = entry
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
			if gh > 0 {
				geohashesInt = append(geohashesInt, gh)
			}
		}

		// Проверка: если только один геохеш, это всё равно должен быть массив
		if len(geohashesInt) == 0 {
			log.Printf("Skipping name %s: no valid geohashes", name)
			continue
		}

		// Сортируем для консистентности
		sort.Slice(geohashesInt, func(i, j int) bool { return geohashesInt[i] < geohashesInt[j] })

		// Убираем дубликаты на всякий случай
		uniqueGeo := make([]uint64, 0, len(geohashesInt))
		seen := make(map[uint64]bool)
		for _, gh := range geohashesInt {
			if !seen[gh] {
				seen[gh] = true
				uniqueGeo = append(uniqueGeo, gh)
			}
		}

		// Конвертируем map в строку через запятую
		geohashesStr := make([]string, 0, len(entry.GeohashesString))
		for gh := range entry.GeohashesString {
			if gh != "" {
				geohashesStr = append(geohashesStr, gh)
			}
		}
		sort.Strings(geohashesStr)

		// Убираем дубликаты строковых геохешей
		uniqueStr := make([]string, 0, len(geohashesStr))
		seenStr := make(map[string]bool)
		for _, gh := range geohashesStr {
			if !seenStr[gh] {
				seenStr[gh] = true
				uniqueStr = append(uniqueStr, gh)
			}
		}

		mvaArray := make([]interface{}, len(uniqueGeo))
		for i, v := range uniqueGeo {
			mvaArray[i] = v
		}

		doc := map[string]interface{}{
			"id":               id,
			"name":             entry.OriginalName, // Используем оригинальное имя
			"geohashes_uint64": mvaArray,
			"geohashes_string": strings.Join(uniqueStr, ","),
			"occurrences":      len(uniqueGeo),
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
