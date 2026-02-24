package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/config"
)

// PathNode представляет узел с путём
type PathNode struct {
	GeonameID int64
	IDs       []int64
	Names     []string
	Depth     int
}

type PathBuilder struct {
	cfg    *config.Config
	client *manticore.ManticoreClient

	// Кэши из существующей иерархии
	relations    map[int64][]int64
	parentMap    map[int64]int64
	geonameCache map[int64]map[string]interface{}
}

func NewPathBuilder(cfg *config.Config, client *manticore.ManticoreClient) *PathBuilder {
	return &PathBuilder{
		cfg:          cfg,
		client:       client,
		relations:    make(map[int64][]int64),
		parentMap:    make(map[int64]int64),
		geonameCache: make(map[int64]map[string]interface{}),
	}
}

const maxDepth = 20 // Разумный максимум для административной иерархии

// BuildPaths строит только материализованные пути на существующей иерархии
func (b *PathBuilder) BuildPaths(ctx context.Context) error {
	log.Println("Starting paths building...")
	start := time.Now()

	// Шаг 1: Загружаем связи из hierarchy
	if err := b.loadRelations(ctx); err != nil {
		return fmt.Errorf("failed to load relations: %w", err)
	}

	// Шаг 2: Загружаем имена геонимов
	if err := b.loadGeonamesNames(ctx); err != nil {
		return fmt.Errorf("failed to load geonames names: %w", err)
	}

	// Шаг 3: Создаём таблицу для путей
	if err := b.ensurePathsTable(ctx); err != nil {
		return fmt.Errorf("failed to ensure paths table: %w", err)
	}

	// Шаг 4: Строим материализованные пути
	if err := b.buildMaterializedPaths(ctx); err != nil {
		return fmt.Errorf("failed to build paths: %w", err)
	}

	log.Printf("Paths building completed in %v", time.Since(start))
	return nil
}

// loadRelations загружает связи из таблицы hierarchy с повторными попытками
func (b *PathBuilder) loadRelations(ctx context.Context) error {
	log.Println("Loading hierarchy relations...")

	lastID := int64(0)
	limit := 100000
	total := 0
	maxRetries := 3

	for {
		var hitsList []interface{}
		var err error

		// Попытка выполнить запрос с ретраями
		for attempt := 0; attempt < maxRetries; attempt++ {
			if attempt > 0 {
				waitTime := time.Second * time.Duration(attempt+1)
				log.Printf("Retry %d for hierarchy relations after %v", attempt+1, waitTime)
				time.Sleep(waitTime)
			}

			hitsList, err = b.fetchHierarchyBatch(ctx, lastID, limit)
			if err == nil {
				break
			}
			log.Printf("Batch failed (attempt %d): %v", attempt+1, err)
		}

		if err != nil {
			return fmt.Errorf("failed after %d attempts at ID %d: %w", maxRetries, lastID, err)
		}

		if len(hitsList) == 0 {
			log.Println("No more hierarchy relations")
			break
		}

		for _, hit := range hitsList {
			hitMap := hit.(map[string]interface{})

			// ID для пагинации
			lastID = int64(hitMap["_id"].(float64))

			// Данные в _source
			source := hitMap["_source"].(map[string]interface{})
			parentID := int64(source["parent_id"].(float64))
			childID := int64(source["child_id"].(float64))

			b.relations[parentID] = append(b.relations[parentID], childID)
			b.parentMap[childID] = parentID
			total++
		}

		log.Printf("Loaded %d hierarchy relations...", total)

		if len(hitsList) < limit {
			break
		}

		// Небольшая задержка между батчами
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("Loaded %d hierarchy relations total", total)
	return nil
}

// fetchHierarchyBatch загружает один батч иерархии
func (b *PathBuilder) fetchHierarchyBatch(ctx context.Context, lastID int64, limit int) ([]interface{}, error) {
	searchReq := map[string]interface{}{
		"index": "hierarchy",
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"id": map[string]interface{}{
					"gt": lastID,
				},
			},
		},
		"_source": []string{"parent_id", "child_id"},
		"sort": []map[string]string{
			{"id": "asc"},
		},
		"size":        limit,
		"max_matches": limit,
	}

	body, _ := json.Marshal(searchReq)

	// Создаем новый клиент для каждого запроса с таймаутом
	client := &http.Client{Timeout: 30 * time.Second}

	resp, err := client.Post(
		fmt.Sprintf("http://%s:%d/json/search", b.cfg.ManticoreHost, b.cfg.ManticorePort),
		"application/json",
		bytes.NewReader(body),
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

	return hitsList, nil
}

// loadGeonamesNames загружает имена геонимов с повторными попытками
func (b *PathBuilder) loadGeonamesNames(ctx context.Context) error {
	log.Println("Loading geonames names...")

	lastID := int64(0)
	limit := 100000
	total := 0
	maxRetries := 3

	for {
		var hitsList []interface{}
		var err error

		// Попытка выполнить запрос с ретраями
		for attempt := 0; attempt < maxRetries; attempt++ {
			if attempt > 0 {
				waitTime := time.Second * time.Duration(attempt+1)
				log.Printf("Retry %d for geonames after %v", attempt+1, waitTime)
				time.Sleep(waitTime)
			}

			hitsList, err = b.fetchGeonamesBatch(ctx, lastID, limit)
			if err == nil {
				break
			}
			log.Printf("Batch failed (attempt %d): %v", attempt+1, err)
		}

		if err != nil {
			return fmt.Errorf("failed after %d attempts at ID %d: %w", maxRetries, lastID, err)
		}

		if len(hitsList) == 0 {
			log.Println("No more geonames")
			break
		}

		for _, hit := range hitsList {
			hitMap := hit.(map[string]interface{})

			id := int64(hitMap["_id"].(float64))
			source := hitMap["_source"].(map[string]interface{})
			name := source["name"].(string)

			b.geonameCache[id] = map[string]interface{}{
				"id":   id,
				"name": name,
			}
			lastID = id
			total++
		}

		if total%100000 == 0 {
			log.Printf("Loaded %d geonames...", total)
		}

		if len(hitsList) < limit {
			break
		}

		// Небольшая задержка между батчами
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("Loaded %d geonames total", total)
	return nil
}

// fetchGeonamesBatch загружает один батч геонимов
func (b *PathBuilder) fetchGeonamesBatch(ctx context.Context, lastID int64, limit int) ([]interface{}, error) {
	searchReq := map[string]interface{}{
		"index": "geonames",
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"id": map[string]interface{}{
					"gt": lastID,
				},
			},
		},
		"_source": []string{"name"},
		"sort": []map[string]string{
			{"id": "asc"},
		},
		"size":        limit,
		"max_matches": limit,
	}

	body, _ := json.Marshal(searchReq)

	client := &http.Client{Timeout: 30 * time.Second}

	resp, err := client.Post(
		fmt.Sprintf("http://%s:%d/json/search", b.cfg.ManticoreHost, b.cfg.ManticorePort),
		"application/json",
		bytes.NewReader(body),
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

	return hitsList, nil
}

// ensurePathsTable создаёт таблицу hierarchy_paths если не существует
func (b *PathBuilder) ensurePathsTable(ctx context.Context) error {
	sql := `CREATE TABLE IF NOT EXISTS hierarchy_paths (
        id bigint,
        geoname_id bigint,
        path string indexed,
        path_ids string indexed,
        depth int,
        root_id bigint
    )`

	_, _, err := b.client.GetClient().UtilsAPI.Sql(ctx).Body(sql).RawResponse(true).Execute()
	if err != nil {
		return fmt.Errorf("failed to create paths table: %w", err)
	}

	log.Println("Ensured hierarchy_paths table exists")
	return nil
}

// buildMaterializedPaths строит пути итеративно, без рекурсии
func (b *PathBuilder) buildMaterializedPaths(ctx context.Context) error {
	log.Println("Building materialized paths iteratively...")

	// Находим корневые элементы
	roots := b.findRoots()
	log.Printf("Found %d root nodes", len(roots))

	batchSize := 10000
	pathBatch := make([]map[string]interface{}, 0, batchSize)
	updateBatch := make([]map[string]interface{}, 0, batchSize)
	totalNodes := 0

	// Для каждого корня строим дерево итеративно через очередь
	for _, rootID := range roots {
		rootName := b.getName(rootID)
		if rootName == "" {
			rootName = fmt.Sprintf("Node_%d", rootID)
		}

		// Очередь для BFS обхода
		type queueItem struct {
			nodeID    int64
			pathIDs   []int64
			pathNames []string
			depth     int
		}

		queue := []queueItem{{
			nodeID:    rootID,
			pathIDs:   []int64{rootID},
			pathNames: []string{rootName},
			depth:     0,
		}}

		for len(queue) > 0 {
			// Извлекаем первый элемент
			item := queue[0]
			queue = queue[1:]

			// Сохраняем путь для текущего узла
			pathDoc := map[string]interface{}{
				"geoname_id": item.nodeID,
				"path":       strings.Join(item.pathNames, "/"),
				"path_ids":   strings.Join(intsToStrings(item.pathIDs), "/"),
				"depth":      item.depth,
				"root_id":    rootID,
			}
			pathBatch = append(pathBatch, pathDoc)

			updateDoc := map[string]interface{}{
				"id":             item.nodeID,
				"hierarchy_path": strings.Join(item.pathNames, "/"),
			}
			updateBatch = append(updateBatch, updateDoc)

			totalNodes++

			// Добавляем детей в очередь
			for _, childID := range b.relations[item.nodeID] {

				// При добавлении в очередь:
				if item.depth >= maxDepth {
					log.Printf("WARNING: Max depth %d reached for node %d", maxDepth, childID)
					continue
				}

				// Проверяем, не было ли уже этого ID в пути (защита от циклов)
				cycleDetected := false
				for _, id := range item.pathIDs {
					if id == childID {
						cycleDetected = true
						log.Printf("WARNING: Cycle detected for node %d, skipping", childID)
						break
					}
				}
				if cycleDetected {
					continue
				}

				childName := b.getName(childID)
				if childName == "" {
					childName = fmt.Sprintf("Node_%d", childID)
				}

				// Создаём новые срезы для пути ребёнка
				childPathIDs := make([]int64, len(item.pathIDs)+1)
				copy(childPathIDs, item.pathIDs)
				childPathIDs[len(item.pathIDs)] = childID

				childPathNames := make([]string, len(item.pathNames)+1)
				copy(childPathNames, item.pathNames)
				childPathNames[len(item.pathNames)] = childName

				queue = append(queue, queueItem{
					nodeID:    childID,
					pathIDs:   childPathIDs,
					pathNames: childPathNames,
					depth:     item.depth + 1,
				})
			}

			// Сохраняем батч когда накопили достаточно
			if len(pathBatch) >= batchSize {
				if err := b.saveBatch(ctx, pathBatch, updateBatch); err != nil {
					return err
				}
				log.Printf("Saved %d paths, memory: %d in queue", totalNodes, len(queue))
				pathBatch = pathBatch[:0]
				updateBatch = updateBatch[:0]

				// Небольшая задержка чтобы не перегружать Manticore
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	// Финальный батч
	if len(pathBatch) > 0 {
		if err := b.saveBatch(ctx, pathBatch, updateBatch); err != nil {
			return err
		}
	}

	log.Printf("Materialized paths built for %d nodes", totalNodes)
	return nil
}

// type PathNode struct {
// 	GeonameID int64
// 	IDs       []int64
// 	Names     []string
// 	Depth     int
// }

// buildTreePaths рекурсивно строит пути
func (b *PathBuilder) buildTreePaths(nodeID int64, nodeName string, parentIDs []int64, parentNames []string, depth int) []PathNode {
	var paths []PathNode

	currentIDs := append(parentIDs, nodeID)
	currentNames := append(parentNames, nodeName)

	paths = append(paths, PathNode{
		GeonameID: nodeID,
		IDs:       currentIDs,
		Names:     currentNames,
		Depth:     depth,
	})

	for _, childID := range b.relations[nodeID] {
		childName := b.getName(childID)
		if childName != "" {
			childPaths := b.buildTreePaths(childID, childName, currentIDs, currentNames, depth+1)
			paths = append(paths, childPaths...)
		}
	}

	return paths
}

// findRoots находит корневые элементы
func (b *PathBuilder) findRoots() []int64 {
	roots := make([]int64, 0)
	seen := make(map[int64]bool)

	for nodeID := range b.relations {
		if _, hasParent := b.parentMap[nodeID]; !hasParent {
			if !seen[nodeID] {
				roots = append(roots, nodeID)
				seen[nodeID] = true
			}
		}
	}

	return roots
}

// getName возвращает имя геонима
func (b *PathBuilder) getName(id int64) string {
	if geoname, ok := b.geonameCache[id]; ok {
		if name, ok := geoname["name"].(string); ok {
			return name
		}
	}
	return ""
}

// saveBatch сохраняет батчи данных
func (b *PathBuilder) saveBatch(ctx context.Context, pathBatch, updateBatch []map[string]interface{}) error {
	// Вставляем в hierarchy_paths
	if err := b.client.BulkInsert(ctx, "hierarchy_paths", pathBatch); err != nil {
		return fmt.Errorf("failed to insert paths: %w", err)
	}

	// Обновляем geonames
	if err := b.client.BulkUpdateGeonames(ctx, updateBatch); err != nil {
		return fmt.Errorf("failed to update geonames: %w", err)
	}

	return nil
}

func intsToStrings(i []int64) []string {
	result := make([]string, len(i))
	for idx, val := range i {
		result[idx] = strconv.FormatInt(val, 10)
	}
	return result
}
