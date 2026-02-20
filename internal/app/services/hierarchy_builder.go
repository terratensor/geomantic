package services

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/config"
	"github.com/terratensor/geomantic/internal/core/domain"
)

type HierarchyBuilder struct {
	cfg    *config.Config
	client *manticore.ManticoreClient

	// Кэши
	geonameCache   map[int64]*domain.Geoname
	childrenCache  map[int64][]int64
	adminCodeCache map[string]int64 // admin code -> geonameID
}

func NewHierarchyBuilder(cfg *config.Config, client *manticore.ManticoreClient) *HierarchyBuilder {
	return &HierarchyBuilder{
		cfg:            cfg,
		client:         client,
		geonameCache:   make(map[int64]*domain.Geoname),
		childrenCache:  make(map[int64][]int64),
		adminCodeCache: make(map[string]int64),
	}
}

// BuildHierarchy строит полную иерархию
func (b *HierarchyBuilder) BuildHierarchy(ctx context.Context) error {
	log.Println("Starting hierarchy building...")
	start := time.Now()

	// Шаг 1: Загружаем admin коды
	if err := b.loadAdminCodes(ctx); err != nil {
		return fmt.Errorf("failed to load admin codes: %w", err)
	}

	// Шаг 2: Загружаем связи из hierarchy таблицы
	if err := b.loadHierarchyRelations(ctx); err != nil {
		return fmt.Errorf("failed to load hierarchy relations: %w", err)
	}

	// Шаг 3: Строим административную иерархию из admin кодов
	if err := b.buildAdminHierarchy(ctx); err != nil {
		return fmt.Errorf("failed to build admin hierarchy: %w", err)
	}

	// Шаг 4: Обновляем parent_id в geonames
	if err := b.updateParentIDs(ctx); err != nil {
		return fmt.Errorf("failed to update parent IDs: %w", err)
	}

	// Шаг 5: Строим материализованные пути
	if err := b.buildMaterializedPaths(ctx); err != nil {
		return fmt.Errorf("failed to build materialized paths: %w", err)
	}

	log.Printf("Hierarchy building completed in %v", time.Since(start))
	return nil
}

// loadAdminCodes загружает admin коды из файлов
func (b *HierarchyBuilder) loadAdminCodes(ctx context.Context) error {
	log.Println("Loading admin codes...")

	// Загружаем admin1Codes
	// Формат: code, name, asciiname, geonameId
	// Пример: AD.02	Canillo	Canillo	304156

	// TODO: Реализовать парсинг admin1CodesASCII.txt

	// Загружаем admin2Codes
	// Формат: concatenated codes, name, asciiname, geonameId
	// Пример: AD.02.01	Canillo	Canillo	304156

	return nil
}

// loadHierarchyRelations загружает связи из таблицы hierarchy
func (b *HierarchyBuilder) loadHierarchyRelations(ctx context.Context) error {
	log.Println("Loading hierarchy relations...")

	// Получаем все связи из таблицы hierarchy
	// TODO: Выполнить SQL запрос к Manticore для получения всех связей

	return nil
}

// buildAdminHierarchy строит иерархию на основе admin кодов
func (b *HierarchyBuilder) buildAdminHierarchy(ctx context.Context) error {
	log.Println("Building admin hierarchy...")

	// Для каждого geoname с admin кодами, находим родителя
	// Например: admin2_code -> admin1_code -> country

	return nil
}

// updateParentIDs обновляет parent_id в таблице geonames
func (b *HierarchyBuilder) updateParentIDs(ctx context.Context) error {
	log.Println("Updating parent IDs...")

	batchSize := 1000
	var batch []map[string]interface{}

	for geonameID, parentID := range b.parentMap() {
		batch = append(batch, map[string]interface{}{
			"id":        geonameID,
			"parent_id": parentID,
		})

		if len(batch) >= batchSize {
			if err := b.client.BulkUpdateGeonames(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := b.client.BulkUpdateGeonames(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

// buildMaterializedPaths строит пути для быстрой навигации
func (b *HierarchyBuilder) buildMaterializedPaths(ctx context.Context) error {
	log.Println("Building materialized paths...")

	// Находим все корневые элементы (континенты и страны)
	roots := b.findRoots()

	for _, root := range roots {
		if err := b.buildPathTree(ctx, root, []int64{}, []string{}, 0); err != nil {
			return err
		}
	}

	return nil
}

// buildPathTree рекурсивно строит пути
func (b *HierarchyBuilder) buildPathTree(ctx context.Context, geonameID int64,
	parentIDs []int64, parentNames []string, depth int) error {

	geoname := b.geonameCache[geonameID]
	if geoname == nil {
		return nil
	}

	// Строим текущий путь
	pathIDs := append(parentIDs, geonameID)
	pathNames := append(parentNames, geoname.Name)

	// TODO: Сохранить путь в Manticore
	// Пока просто логируем для отладки
	if depth <= 2 { // Логируем только верхние уровни
		log.Printf("Path: %s -> %s", strings.Join(intsToStrings(parentIDs), "/"), geoname.Name)
	}

	// Рекурсивно обрабатываем детей
	for _, childID := range b.childrenCache[geonameID] {
		if err := b.buildPathTree(ctx, childID, pathIDs, pathNames, depth+1); err != nil {
			return err
		}
	}

	return nil
}

// Вспомогательные функции
// parentMap собирает map geonameID -> parentID из всех источников
func (b *HierarchyBuilder) parentMap() map[int64]int64 {
	parentMap := make(map[int64]int64)

	// Из иерархических связей
	for parentID, children := range b.childrenCache {
		for _, childID := range children {
			parentMap[childID] = parentID
		}
	}

	return parentMap
}

func (b *HierarchyBuilder) findRoots() []int64 {
	var roots []int64
	for id, geoname := range b.geonameCache {
		if geoname.FeatureCode == "CONT" || // континент
			geoname.FeatureCode == "PCLI" || // независимое государство
			(geoname.ParentID == nil && b.hasChildren(id)) {
			roots = append(roots, id)
		}
	}
	return roots
}

func (b *HierarchyBuilder) hasChildren(id int64) bool {
	return len(b.childrenCache[id]) > 0
}

// intsToStrings конвертирует []int64 в []string
func intsToStrings(ints []int64) []string {
	strs := make([]string, len(ints))
	for i, v := range ints {
		strs[i] = fmt.Sprintf("%d", v)
	}
	return strs
}
