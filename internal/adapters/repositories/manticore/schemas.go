package manticore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	manticoresearch "github.com/manticoresoftware/manticoresearch-go"
	"github.com/terratensor/geomantic/internal/core/domain"
)

const (
	TableGeonames       = "geonames"
	TableAlternateNames = "alternate_names"
	TableHierarchy      = "hierarchy"
	TableAdminCodes     = "admin_codes"
	TableNameDict       = "geoname_dict"
)

var CreateTablesSQL = []string{
	// Таблица геонимов с полной схемой
	fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
        id bigint,
        name text,
        asciiname string attribute indexed,
        alternatenames text,
        latitude float,
        longitude float,
        feature_class string,
        feature_code string,
        country_code string,
        cc2 string,
        admin1_code string,
        admin2_code string,
        admin3_code string,
        admin4_code string,
        population bigint,
        elevation int,
        dem int,
        timezone string,
        modification_date timestamp,
        parent_id bigint,
        hierarchy_path string,
        full_text text,
		geohash_int bigint,
        geohash_string string attribute indexed
    ) 
    morphology='lemmatize_ru_all, stem_enru'
    min_stemming_len='4'
    index_exact_words='1'
    min_infix_len='3'
    expand_keywords='1'
    html_strip='1'`, TableGeonames),

	// Таблица альтернативных имен
	fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
        id bigint,
        geonameid bigint,
        isolanguage string attribute indexed,
        alternatename text,
        ispreferredname bool,
        isshortname bool,
        iscolloquial bool,
        ishistoric bool,
        from_period string,
        to_period string,
        language_group string,
        is_official bool
    )
    morphology='lemmatize_ru_all, stem_enru'
    min_infix_len='2'
    index_exact_words='1'`, TableAlternateNames),

	// Таблица иерархии
	`CREATE TABLE IF NOT EXISTS hierarchy (
        id bigint,
        parent_id bigint,
        child_id bigint,
        relation_type string,
        is_admin bool,
        is_user_defined bool
    )`,

	// Таблица для материализованных путей (опционально)
	`CREATE TABLE IF NOT EXISTS hierarchy_paths (
        id bigint,
        geoname_id bigint,
        path text,
        path_ids text,
        depth int,
        root_id bigint
    )`,

	// Таблица для admin кодов
	`CREATE TABLE IF NOT EXISTS admin_codes (
        id bigint,
        code string indexed,
        name text,
        ascii_name string,
        geoname_id bigint,
        level int,
        parent_code string
    )`,

	// Таблица словаря имён
	fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
        id bigint,
        name text,
        geohashes_uint64 multi64,
        geohashes_string text,
        occurrences int,
        first_geoname_id bigint
    ) 
    morphology='lemmatize_ru_all, stem_enru'
    min_infix_len='2'
    html_strip='1'`, TableNameDict),
}

type ManticoreClient struct {
	client *manticoresearch.APIClient
}

func NewClient(host string, port int) (*ManticoreClient, error) {
	configuration := manticoresearch.NewConfiguration()
	configuration.Servers = manticoresearch.ServerConfigurations{
		{
			URL: fmt.Sprintf("http://%s:%d", host, port),
		},
	}

	// Увеличиваем таймауты для больших bulk операций
	configuration.HTTPClient = &http.Client{
		Timeout: 5 * time.Minute,
	}

	client := manticoresearch.NewAPIClient(configuration)

	return &ManticoreClient{
		client: client,
	}, nil
}

// GetClient возвращает внутренний клиент Manticore
func (c *ManticoreClient) GetClient() *manticoresearch.APIClient {
	return c.client
}

// InitSchema создает таблицы если они не существуют
func (c *ManticoreClient) InitSchema(ctx context.Context) error {
	for _, sql := range CreateTablesSQL {
		// Для создания таблиц нужно использовать POST запрос к /sql
		req := c.client.UtilsAPI.Sql(ctx).Body(sql)
		req = req.RawResponse(true)

		resp, httpResp, err := c.client.UtilsAPI.SqlExecute(req)
		if err != nil {
			// Проверим детали ошибки
			if httpResp != nil {
				body, _ := io.ReadAll(httpResp.Body)
				return fmt.Errorf("failed to execute SQL: %w, response: %s", err, string(body))
			}
			return fmt.Errorf("failed to execute SQL: %w", err)
		}

		if httpResp != nil && httpResp.StatusCode != 200 {
			return fmt.Errorf("init schema returned HTTP %d", httpResp.StatusCode)
		}

		// Проверяем наличие ошибок в ответе
		if resp != nil && resp.SqlObjResponse != nil {
			hits := resp.SqlObjResponse.GetHits()
			if error, ok := hits["error"]; ok && error != nil {
				return fmt.Errorf("SQL error: %v", error)
			}
		}
	}
	return nil
}

// geonameToMap конвертирует доменную модель в map для Manticore
func geonameToMap(g *domain.Geoname) map[string]interface{} {
	doc := map[string]interface{}{
		"id":                g.ID, // id обязательно должен быть
		"name":              g.Name,
		"asciiname":         g.ASCIIName,
		"alternatenames":    strings.Join(g.AlternateNames, ","),
		"latitude":          g.Latitude,
		"longitude":         g.Longitude,
		"feature_class":     g.FeatureClass,
		"feature_code":      g.FeatureCode,
		"country_code":      g.CountryCode,
		"cc2":               strings.Join(g.CC2, ","),
		"admin1_code":       g.Admin1Code,
		"admin2_code":       g.Admin2Code,
		"admin3_code":       g.Admin3Code,
		"admin4_code":       g.Admin4Code,
		"population":        g.Population,
		"dem":               g.DEM,
		"timezone":          g.Timezone,
		"modification_date": g.ModificationDate.Unix(),
		"full_text":         g.FullText(),
		"geohash_int":       g.GeohashInt,
		"geohash_string":    g.GeohashString,
	}

	if g.Elevation != nil {
		doc["elevation"] = *g.Elevation
	}

	if g.ParentID != nil {
		doc["parent_id"] = *g.ParentID
	}

	if g.HierarchyPath != "" {
		doc["hierarchy_path"] = g.HierarchyPath
	}

	return doc
}

// alternateNameToMap конвертирует доменную модель в map для Manticore
func alternateNameToMap(a *domain.AlternateName) map[string]interface{} {
	doc := map[string]interface{}{
		"id":              a.ID,
		"geonameid":       a.GeonameID,
		"isolanguage":     a.ISOLanguage,
		"alternatename":   a.AlternateName,
		"ispreferredname": a.IsPreferredName,
		"isshortname":     a.IsShortName,
		"iscolloquial":    a.IsColloquial,
		"ishistoric":      a.IsHistoric,
		"language_group":  a.LanguageGroup(),
		"is_official":     a.IsPreferredName || a.ISOLanguage == "ru" || a.ISOLanguage == "en",
	}

	if a.From != nil {
		doc["from_period"] = *a.From
	}

	if a.To != nil {
		doc["to_period"] = *a.To
	}

	return doc
}

// hierarchyRelationToMap конвертирует доменную модель в map для Manticore
func hierarchyRelationToMap(r *domain.HierarchyRelation) map[string]interface{} {
	return map[string]interface{}{
		"parent_id":       r.ParentID,
		"child_id":        r.ChildID,
		"relation_type":   r.RelationType,
		"is_admin":        r.IsAdmin(),
		"is_user_defined": r.IsUserDefined(),
	}
}

// BulkInsertGeonames вставляет геонимы пачкой
func (c *ManticoreClient) BulkInsertGeonames(ctx context.Context, geonames []*domain.Geoname) error {
	if len(geonames) == 0 {
		return nil
	}

	docs := make([]map[string]interface{}, len(geonames))
	for i, g := range geonames {
		docs[i] = geonameToMap(g)
	}

	return c.bulkInsert(ctx, TableGeonames, docs)
}

// BulkInsertAlternateNames вставляет альтернативные имена пачкой
func (c *ManticoreClient) BulkInsertAlternateNames(ctx context.Context, altNames []*domain.AlternateName) error {
	if len(altNames) == 0 {
		return nil
	}

	docs := make([]map[string]interface{}, len(altNames))
	for i, a := range altNames {
		docs[i] = alternateNameToMap(a)
	}

	return c.bulkInsert(ctx, TableAlternateNames, docs)
}

// BulkInsertHierarchyRelations вставляет связи иерархии пачкой
func (c *ManticoreClient) BulkInsertHierarchyRelations(ctx context.Context, docs []map[string]interface{}) error {
	if len(docs) == 0 {
		return nil
	}

	// Проверяем существование таблицы
	exists, err := c.TableExists(ctx, "hierarchy")
	if err != nil {
		return err
	}

	if !exists {
		if err := c.createHierarchyTable(ctx); err != nil {
			return err
		}
	}

	return c.bulkInsert(ctx, "hierarchy", docs)
}

// createHierarchyTable создает таблицу для иерархии
func (c *ManticoreClient) createHierarchyTable(ctx context.Context) error {
	sql := `CREATE TABLE IF NOT EXISTS hierarchy (
        id bigint,
        parent_id bigint,
        child_id bigint,
        relation_type string,
        is_admin bool,
        is_user_defined bool
    )`

	req := c.client.UtilsAPI.Sql(ctx).Body(sql)
	req = req.RawResponse(true)

	_, httpResp, err := c.client.UtilsAPI.SqlExecute(req)
	if err != nil {
		return fmt.Errorf("failed to create hierarchy table: %w", err)
	}

	if httpResp != nil && httpResp.StatusCode != 200 {
		return fmt.Errorf("create hierarchy table returned HTTP %d", httpResp.StatusCode)
	}

	// Логируем только один раз при создании
	log.Println("Created hierarchy table")
	return nil
}

// BulkInsert вставляет документы в указанную таблицу
func (c *ManticoreClient) BulkInsert(ctx context.Context, table string, docs []map[string]interface{}) error {
	if len(docs) == 0 {
		return nil
	}

	// Проверяем существование таблицы
	exists, err := c.TableExists(ctx, table)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("table %s does not exist", table)
	}

	return c.bulkInsert(ctx, table, docs)
}

// bulkInsert общая функция для вставки документов
func (c *ManticoreClient) bulkInsert(ctx context.Context, table string, docs []map[string]interface{}) error {
	if len(docs) == 0 {
		return nil
	}

	// Создаем NDJSON буфер
	var buf bytes.Buffer

	for _, doc := range docs {
		// Проверяем, есть ли поле id
		hasID := false
		idVal := doc["id"]

		// Создаем копию документа без id для поля doc
		docWithoutID := make(map[string]interface{})
		for k, v := range doc {
			if k != "id" {
				docWithoutID[k] = v
			} else {
				hasID = true
			}
		}

		// Создаем команду insert
		insertObj := map[string]interface{}{
			"table": table,
			"doc":   docWithoutID,
		}

		// Добавляем id только если он явно указан
		if hasID {
			insertObj["id"] = idVal
		}

		insertCmd := map[string]interface{}{
			"insert": insertObj,
		}

		cmdBytes, err := json.Marshal(insertCmd)
		if err != nil {
			return fmt.Errorf("failed to marshal insert command: %w", err)
		}

		buf.Write(cmdBytes)
		buf.WriteByte('\n')
	}

	// Делаем прямой HTTP запрос
	url := fmt.Sprintf("http://%s:%d/bulk", "localhost", 9308)

	req, err := http.NewRequestWithContext(ctx, "POST", url, &buf)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-ndjson")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Читаем ответ
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("bulk insert returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Парсим ответ
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	// Проверяем на ошибки
	if errors, ok := response["errors"]; ok && errors == true {
		if error, ok := response["error"]; ok && error != nil {
			return fmt.Errorf("bulk insert error: %v", error)
		}
		return fmt.Errorf("bulk insert completed with errors: %v", response)
	}

	return nil
}

// InsertGeoname вставляет один геоним
func (c *ManticoreClient) InsertGeoname(ctx context.Context, geoname *domain.Geoname) error {
	doc := geonameToMap(geoname)

	insertReq := manticoresearch.NewInsertDocumentRequest(TableGeonames, doc)
	// Преобразуем int64 в uint64 для ID
	id := uint64(geoname.ID)
	insertReq.Id = &id

	req := c.client.IndexAPI.Insert(ctx).InsertDocumentRequest(*insertReq)
	resp, _, err := c.client.IndexAPI.InsertExecute(req)
	if err != nil {
		return fmt.Errorf("failed to insert geoname: %w", err)
	}

	// SuccessResponse не имеет поля Error, проверяем через Result или статус
	if resp == nil {
		return fmt.Errorf("insert returned nil response")
	}

	return nil
}

// UpdateGeoname обновляет один геоним
func (c *ManticoreClient) UpdateGeoname(ctx context.Context, geoname *domain.Geoname) error {
	doc := geonameToMap(geoname)

	updateReq := manticoresearch.NewUpdateDocumentRequest(TableGeonames, doc)
	// Преобразуем int64 в uint64 для ID
	id := uint64(geoname.ID)
	updateReq.Id = &id

	req := c.client.IndexAPI.Update(ctx).UpdateDocumentRequest(*updateReq)
	resp, _, err := c.client.IndexAPI.UpdateExecute(req)
	if err != nil {
		return fmt.Errorf("failed to update geoname: %w", err)
	}

	// UpdateResponse не имеет поля Error
	if resp == nil {
		return fmt.Errorf("update returned nil response")
	}

	return nil
}

// DeleteGeoname удаляет один геоним
func (c *ManticoreClient) DeleteGeoname(ctx context.Context, id int64) error {
	deleteReq := manticoresearch.NewDeleteDocumentRequest(TableGeonames)
	// Преобразуем int64 в uint64 для ID
	uid := uint64(id)
	deleteReq.Id = &uid

	req := c.client.IndexAPI.Delete(ctx).DeleteDocumentRequest(*deleteReq)
	resp, _, err := c.client.IndexAPI.DeleteExecute(req)
	if err != nil {
		return fmt.Errorf("failed to delete geoname: %w", err)
	}

	// DeleteResponse не имеет поля Error
	if resp == nil {
		return fmt.Errorf("delete returned nil response")
	}

	return nil
}

// TableExists проверяет существование таблицы через SHOW CREATE TABLE
func (c *ManticoreClient) TableExists(ctx context.Context, tableName string) (bool, error) {
	showCreateTableQuery := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	req := c.client.UtilsAPI.Sql(ctx).Body(showCreateTableQuery)
	req = req.RawResponse(true)

	_, _, err := c.client.UtilsAPI.SqlExecute(req)

	if err == nil {
		return true, nil
	}

	// Если ошибка - таблицы нет
	return false, nil
}

// DropTable удаляет таблицу
func (c *ManticoreClient) DropTable(ctx context.Context, tableName string) error {
	req := c.client.UtilsAPI.Sql(ctx).Body(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	_, _, err := c.client.UtilsAPI.SqlExecute(req)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}
	return nil
}

// TruncateTable очищает таблицу
func (c *ManticoreClient) TruncateTable(ctx context.Context, tableName string) error {
	req := c.client.UtilsAPI.Sql(ctx).Body(fmt.Sprintf("TRUNCATE TABLE %s", tableName))
	_, _, err := c.client.UtilsAPI.SqlExecute(req)
	if err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}
	return nil
}

// GetTableCount возвращает количество документов в таблице
func (c *ManticoreClient) GetTableCount(ctx context.Context, tableName string) (int64, error) {
	req := c.client.UtilsAPI.Sql(ctx).Body(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	// Устанавливаем rawResponse=true для получения структурированного ответа
	req = req.RawResponse(true)

	resp, httpResp, err := c.client.UtilsAPI.SqlExecute(req)
	if err != nil {
		return 0, fmt.Errorf("failed to get table count: %w", err)
	}

	// Проверяем HTTP статус
	if httpResp != nil && httpResp.StatusCode != 200 {
		return 0, fmt.Errorf("get table count returned HTTP %d", httpResp.StatusCode)
	}

	// Получаем Hits из ответа
	hits := resp.SqlObjResponse.GetHits()

	// Извлекаем данные из Hits
	if data, ok := hits["data"]; ok {
		if rows, ok := data.([]interface{}); ok && len(rows) > 0 {
			if row, ok := rows[0].(map[string]interface{}); ok {
				if count, ok := row["count(*)"]; ok {
					// count может быть float64 или int64
					switch v := count.(type) {
					case float64:
						return int64(v), nil
					case int64:
						return v, nil
					case int:
						return int64(v), nil
					}
				}
			}
		}
	}

	return 0, nil
}

// BulkUpdateGeonames обновляет поля в geonames пачкой
func (c *ManticoreClient) BulkUpdateGeonames(ctx context.Context, updates []map[string]interface{}) error {
	if len(updates) == 0 {
		return nil
	}

	// Создаем NDJSON буфер
	var buf bytes.Buffer

	for _, update := range updates {
		id, ok := update["id"]
		if !ok {
			continue
		}

		// Удаляем id из документа для обновления
		doc := make(map[string]interface{})
		for k, v := range update {
			if k != "id" {
				doc[k] = v
			}
		}

		updateCmd := map[string]interface{}{
			"update": map[string]interface{}{
				"table": TableGeonames,
				"id":    id,
				"doc":   doc,
			},
		}

		cmdBytes, err := json.Marshal(updateCmd)
		if err != nil {
			return fmt.Errorf("failed to marshal update command: %w", err)
		}

		buf.Write(cmdBytes)
		buf.WriteByte('\n')
	}

	return c.bulkRequest(ctx, buf.Bytes())
}

// bulkRequest выполняет HTTP запрос к Manticore с ретраем
func (c *ManticoreClient) bulkRequest(ctx context.Context, data []byte) error {
	url := fmt.Sprintf("http://%s:%d/bulk", "localhost", 9308)

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Content-Type", "application/x-ndjson")

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			if attempt < 2 {
				time.Sleep(time.Second * time.Duration(attempt+1))
				continue
			}
			return fmt.Errorf("failed to send request after %d attempts: %w", attempt+1, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("bulk request returned HTTP %d: %s", resp.StatusCode, string(body))
		}

		return nil
	}

	return lastErr
}

// BulkInsertAdminCodes вставляет admin коды пачкой
func (c *ManticoreClient) BulkInsertAdminCodes(ctx context.Context, docs []map[string]interface{}) error {
	if len(docs) == 0 {
		return nil
	}

	// Создаем таблицу если не существует
	exists, err := c.TableExists(ctx, "admin_codes")
	if err != nil {
		return err
	}

	if !exists {
		if err := c.createAdminCodesTable(ctx); err != nil {
			return err
		}
	}

	return c.bulkInsert(ctx, "admin_codes", docs)
}

// createAdminCodesTable создает таблицу для admin кодов
func (c *ManticoreClient) createAdminCodesTable(ctx context.Context) error {
	sql := `CREATE TABLE IF NOT EXISTS admin_codes (
        id bigint,
        code string indexed,
        name text,
        ascii_name string,
        geoname_id bigint,
        level int,
        parent_code string
    )`

	req := c.client.UtilsAPI.Sql(ctx).Body(sql)
	req = req.RawResponse(true)

	_, httpResp, err := c.client.UtilsAPI.SqlExecute(req)
	if err != nil {
		return fmt.Errorf("failed to create admin_codes table: %w", err)
	}

	if httpResp != nil && httpResp.StatusCode != 200 {
		return fmt.Errorf("create admin_codes table returned HTTP %d", httpResp.StatusCode)
	}

	log.Println("Created admin_codes table")
	return nil
}

// BulkInsertNames вставляет записи словаря имён пачкой
func (c *ManticoreClient) BulkInsertNames(ctx context.Context, docs []map[string]interface{}) error {
	if len(docs) == 0 {
		return nil
	}

	// Проверяем существование таблицы
	exists, err := c.TableExists(ctx, TableNameDict)
	if err != nil {
		return err
	}

	if !exists {
		if err := c.createNameDictTable(ctx); err != nil {
			return err
		}
	}

	return c.bulkInsert(ctx, TableNameDict, docs)
}

// createNameDictTable создает таблицу для словаря имён
func (c *ManticoreClient) createNameDictTable(ctx context.Context) error {
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
        id bigint,
        name text,
        geohashes_uint64 multi64,
        geohashes_string text,
        occurrences int,
        first_geoname_id bigint
    ) 
    morphology='lemmatize_ru_all, stem_enru'
    min_infix_len='2'
    html_strip='1'`, TableNameDict)

	req := c.client.UtilsAPI.Sql(ctx).Body(sql)
	req = req.RawResponse(true)

	_, httpResp, err := c.client.UtilsAPI.SqlExecute(req)
	if err != nil {
		return fmt.Errorf("failed to create %v table: %w", TableNameDict, err)
	}

	if httpResp != nil && httpResp.StatusCode != 200 {
		return fmt.Errorf("create %v table returned HTTP %d", TableNameDict, httpResp.StatusCode)
	}

	log.Printf("Created %v table (createNameDictTable)", TableNameDict)
	return nil
}

// CreateNameDictTable создает таблицу для словаря имён
func (c *ManticoreClient) CreateNameDictTable(ctx context.Context) error {
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
        id bigint,
        name text,
        geohashes_uint64 multi64,
        geohashes_string text,
        occurrences int,
        first_geoname_id bigint
    ) 
    morphology='lemmatize_ru_all, stem_enru'
    min_infix_len='2'
    html_strip='1'`, TableNameDict)

	req := c.client.UtilsAPI.Sql(ctx).Body(sql)
	req = req.RawResponse(true)

	_, httpResp, err := c.client.UtilsAPI.SqlExecute(req)
	if err != nil {
		return fmt.Errorf("failed to create %v table: %w", TableNameDict, err)
	}

	if httpResp != nil && httpResp.StatusCode != 200 {
		return fmt.Errorf("create %v table returned HTTP %d", TableNameDict, httpResp.StatusCode)
	}

	log.Printf("Created %v table (CreateNameDictTable)", TableNameDict)
	return nil
}
