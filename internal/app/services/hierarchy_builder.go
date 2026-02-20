package services

import (
	"bytes"
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

type HierarchyBuilder struct {
	cfg        *config.Config
	client     *manticore.ManticoreClient
	httpClient *http.Client

	// Кэши
	relations    map[int64][]int64
	parentMap    map[int64]int64
	adminCodeMap map[string]int64
	geonameCache map[int64]map[string]interface{}
}

func NewHierarchyBuilder(cfg *config.Config, client *manticore.ManticoreClient) *HierarchyBuilder {
	return &HierarchyBuilder{
		cfg:          cfg,
		client:       client,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		relations:    make(map[int64][]int64),
		parentMap:    make(map[int64]int64),
		adminCodeMap: make(map[string]int64),
		geonameCache: make(map[int64]map[string]interface{}),
	}
}

// BuildHierarchy строит полную иерархию
func (b *HierarchyBuilder) BuildHierarchy(ctx context.Context) error {
	log.Println("Starting hierarchy building...")
	start := time.Now()

	// Шаг 1: Загружаем геонимы в кэш
	if err := b.loadGeonames(ctx); err != nil {
		return fmt.Errorf("failed to load geonames: %w", err)
	}

	// Шаг 2: Загружаем admin коды
	if err := b.loadAdminCodes(ctx); err != nil {
		return fmt.Errorf("failed to load admin codes: %w", err)
	}

	// Шаг 3: Загружаем связи из hierarchy таблицы
	if err := b.loadHierarchyRelations(ctx); err != nil {
		return fmt.Errorf("failed to load hierarchy relations: %w", err)
	}

	// Шаг 4: Строим административную иерархию
	if err := b.buildAdminHierarchy(ctx); err != nil {
		return fmt.Errorf("failed to build admin hierarchy: %w", err)
	}

	// Шаг 5: Обновляем parent_id в geonames
	if err := b.updateParentIDs(ctx); err != nil {
		return fmt.Errorf("failed to update parent IDs: %w", err)
	}

	log.Printf("Hierarchy building completed in %v", time.Since(start))
	log.Printf("Statistics: %d relations, %d admin codes", len(b.relations), len(b.adminCodeMap))

	return nil
}

// loadGeonames загружает геонимы из Manticore
func (b *HierarchyBuilder) loadGeonames(ctx context.Context) error {
	log.Println("Loading geonames from Manticore...")

	offset := 0
	limit := 1000
	totalLoaded := 0

	for {
		// Создаём поисковый запрос
		searchReq := map[string]interface{}{
			"table": "geonames",
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
			"_source": []string{
				"id", "name", "country_code", "feature_code",
				"admin1_code", "admin2_code", "admin3_code", "admin4_code",
			},
			"limit":  limit,
			"offset": offset,
		}

		body, err := json.Marshal(searchReq)
		if err != nil {
			return fmt.Errorf("failed to marshal search request: %w", err)
		}

		// Выполняем запрос к Manticore
		resp, err := b.httpClient.Post(
			fmt.Sprintf("http://%s:%d/search", b.cfg.ManticoreHost, b.cfg.ManticorePort),
			"application/json",
			bytes.NewReader(body),
		)
		if err != nil {
			return fmt.Errorf("failed to search geonames: %w", err)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		// Извлекаем хиты
		hits, ok := result["hits"].(map[string]interface{})
		if !ok {
			break
		}

		hitsList, ok := hits["hits"].([]interface{})
		if !ok || len(hitsList) == 0 {
			break
		}

		for _, hit := range hitsList {
			hitMap, ok := hit.(map[string]interface{})
			if !ok {
				continue
			}

			source, ok := hitMap["_source"].(map[string]interface{})
			if !ok {
				continue
			}

			// Извлекаем ID
			idFloat, ok := source["id"].(float64)
			if !ok {
				continue
			}
			id := int64(idFloat)

			b.geonameCache[id] = source
			totalLoaded++
		}

		offset += limit
		log.Printf("Loaded %d geonames...", totalLoaded)

		if len(hitsList) < limit {
			break
		}
	}

	log.Printf("Loaded %d geonames total", totalLoaded)
	return nil
}

// loadAdminCodes строит карту admin кодов
func (b *HierarchyBuilder) loadAdminCodes(ctx context.Context) error {
	log.Println("Building admin code map...")

	for id, geoname := range b.geonameCache {
		// Для ADM1 объектов
		if code, ok := geoname["admin1_code"].(string); ok && code != "" && code != "00" {
			countryCode, _ := geoname["country_code"].(string)
			if countryCode != "" {
				adminKey := fmt.Sprintf("%s.%s", countryCode, code)
				b.adminCodeMap[adminKey] = id
			}
		}

		// Для ADM2 объектов
		if code, ok := geoname["admin2_code"].(string); ok && code != "" && code != "00" {
			countryCode, _ := geoname["country_code"].(string)
			admin1Code, _ := geoname["admin1_code"].(string)
			if countryCode != "" && admin1Code != "" && admin1Code != "00" {
				adminKey := fmt.Sprintf("%s.%s.%s", countryCode, admin1Code, code)
				b.adminCodeMap[adminKey] = id
			}
		}
	}

	log.Printf("Loaded %d admin codes", len(b.adminCodeMap))
	return nil
}

// loadHierarchyRelations загружает связи из таблицы hierarchy
func (b *HierarchyBuilder) loadHierarchyRelations(ctx context.Context) error {
	log.Println("Loading hierarchy relations from Manticore...")

	// SQL запрос через HTTP API
	sqlQuery := "SELECT parent_id, child_id, relation_type FROM hierarchy"

	resp, err := b.httpClient.Post(
		fmt.Sprintf("http://%s:%d/sql", b.cfg.ManticoreHost, b.cfg.ManticorePort),
		"text/plain",
		strings.NewReader(sqlQuery),
	)
	if err != nil {
		return fmt.Errorf("failed to execute SQL: %w", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Извлекаем данные
	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil
	}

	data, ok := hits["hits"].([]interface{})
	if !ok {
		return nil
	}

	for _, item := range data {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		source, ok := itemMap["_source"].(map[string]interface{})
		if !ok {
			continue
		}

		parentID := int64(source["parent_id"].(float64))
		childID := int64(source["child_id"].(float64))
		// relType, _ := source["relation_type"].(string)

		b.relations[parentID] = append(b.relations[parentID], childID)
		b.parentMap[childID] = parentID
	}

	log.Printf("Loaded %d hierarchy relations", len(b.parentMap))
	return nil
}

// buildAdminHierarchy строит иерархию на основе admin кодов
func (b *HierarchyBuilder) buildAdminHierarchy(ctx context.Context) error {
	log.Println("Building admin hierarchy...")

	added := 0

	for id, geoname := range b.geonameCache {
		// Пропускаем если уже есть родитель
		if _, exists := b.parentMap[id]; exists {
			continue
		}

		// Определяем родителя по admin кодам
		parentID := b.findParentByAdminCodes(geoname)
		if parentID != 0 && parentID != id {
			b.relations[parentID] = append(b.relations[parentID], id)
			b.parentMap[id] = parentID
			added++
		}
	}

	log.Printf("Added %d admin hierarchy relations", added)
	return nil
}

// findParentByAdminCodes находит родителя по admin кодам
func (b *HierarchyBuilder) findParentByAdminCodes(geoname map[string]interface{}) int64 {
	countryCode, _ := geoname["country_code"].(string)
	if countryCode == "" {
		return 0
	}

	// Пробуем найти родителя по admin4 -> admin3 -> admin2 -> admin1
	if code, ok := geoname["admin4_code"].(string); ok && code != "" && code != "00" {
		if admin3, ok := geoname["admin3_code"].(string); ok && admin3 != "" && admin3 != "00" {
			if admin2, ok := geoname["admin2_code"].(string); ok && admin2 != "" && admin2 != "00" {
				if admin1, ok := geoname["admin1_code"].(string); ok && admin1 != "" && admin1 != "00" {
					key := fmt.Sprintf("%s.%s.%s.%s", countryCode, admin1, admin2, admin3)
					if id, exists := b.adminCodeMap[key]; exists {
						return id
					}
				}
			}
		}
	}

	if code, ok := geoname["admin3_code"].(string); ok && code != "" && code != "00" {
		if admin2, ok := geoname["admin2_code"].(string); ok && admin2 != "" && admin2 != "00" {
			if admin1, ok := geoname["admin1_code"].(string); ok && admin1 != "" && admin1 != "00" {
				key := fmt.Sprintf("%s.%s.%s", countryCode, admin1, admin2)
				if id, exists := b.adminCodeMap[key]; exists {
					return id
				}
			}
		}
	}

	if code, ok := geoname["admin2_code"].(string); ok && code != "" && code != "00" {
		if admin1, ok := geoname["admin1_code"].(string); ok && admin1 != "" && admin1 != "00" {
			key := fmt.Sprintf("%s.%s.%s", countryCode, admin1, code)
			if id, exists := b.adminCodeMap[key]; exists {
				return id
			}
		}
	}

	if code, ok := geoname["admin1_code"].(string); ok && code != "" && code != "00" {
		key := fmt.Sprintf("%s.%s", countryCode, code)
		if id, exists := b.adminCodeMap[key]; exists {
			return id
		}
	}

	return 0
}

// updateParentIDs обновляет parent_id в таблице geonames
func (b *HierarchyBuilder) updateParentIDs(ctx context.Context) error {
	log.Println("Updating parent IDs in Manticore...")

	batchSize := 100
	batch := make([]map[string]interface{}, 0, batchSize)
	updated := 0

	for childID, parentID := range b.parentMap {
		batch = append(batch, map[string]interface{}{
			"id":        childID,
			"parent_id": parentID,
		})

		if len(batch) >= batchSize {
			if err := b.bulkUpdateParentIDs(ctx, batch); err != nil {
				return err
			}
			updated += len(batch)
			log.Printf("Updated %d parent IDs...", updated)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := b.bulkUpdateParentIDs(ctx, batch); err != nil {
			return err
		}
		updated += len(batch)
	}

	log.Printf("Updated %d parent IDs total", updated)
	return nil
}

// bulkUpdateParentIDs выполняет массовое обновление parent_id
func (b *HierarchyBuilder) bulkUpdateParentIDs(ctx context.Context, updates []map[string]interface{}) error {
	// Создаём NDJSON для bulk update
	var buf bytes.Buffer

	for _, update := range updates {
		id := update["id"]
		parentID := update["parent_id"]

		updateCmd := map[string]interface{}{
			"update": map[string]interface{}{
				"table": "geonames",
				"id":    id,
				"doc": map[string]interface{}{
					"parent_id": parentID,
				},
			},
		}

		cmdBytes, err := json.Marshal(updateCmd)
		if err != nil {
			return fmt.Errorf("failed to marshal update command: %w", err)
		}

		buf.Write(cmdBytes)
		buf.WriteByte('\n')
	}

	// Отправляем запрос
	resp, err := b.httpClient.Post(
		fmt.Sprintf("http://%s:%d/bulk", b.cfg.ManticoreHost, b.cfg.ManticorePort),
		"application/x-ndjson",
		&buf,
	)
	if err != nil {
		return fmt.Errorf("failed to execute bulk update: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("bulk update returned HTTP %d", resp.StatusCode)
	}

	return nil
}
