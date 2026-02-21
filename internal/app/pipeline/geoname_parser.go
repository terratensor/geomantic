package pipeline

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/config"
	"github.com/terratensor/geomantic/internal/core/domain"
)

// GeonameParser handles parsing of allCountries.txt
type GeonameParser struct {
	*BaseParser
	batchChan    chan []*domain.Geoname
	geohashLevel int
}

func NewGeonameParser(cfg *config.Config) *GeonameParser {
	return &GeonameParser{
		BaseParser:   NewBaseParser(cfg),
		batchChan:    make(chan []*domain.Geoname, cfg.ChannelBufferSize),
		geohashLevel: cfg.S2GeohashLevel,
	}
}

// ProcessFile processes the geonames file and imports into Manticore
func (p *GeonameParser) ProcessFile(ctx context.Context, filePath string, client *manticore.ManticoreClient) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	log.Printf("Processing geonames file: %s", filepath.Base(filePath))

	// Create progress bar
	bar, err := p.ProgressBar(file, fmt.Sprintf("Processing %s", filepath.Base(filePath)))
	if err != nil {
		return 0, err
	}

	// Start worker goroutines
	errChan := make(chan error, p.workers)
	var processed int64
	var parseErrors int64

	// Start consumer goroutine
	go p.startConsumer(ctx, client, errChan, &processed)

	// Читаем файл построчно (не используем CSV reader)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // Увеличиваем буфер для длинных строк

	batch := make([]*domain.Geoname, 0, p.batchSize)
	var lineCount int64

	for scanner.Scan() {
		line := scanner.Text()

		// Пропускаем пустые строки и комментарии
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Обновляем прогресс (приблизительно)
		bar.Add(len(line) + 1)

		// Разделяем строку вручную, сохраняя пустые поля
		fields := strings.Split(line, "\t")

		// Нормализуем до 19 полей
		for len(fields) < 19 {
			fields = append(fields, "")
		}

		// Parse geoname from fields
		geoname, err := p.parseGeonameFromFields(fields, lineCount+1)
		if err != nil {
			parseErrors++
			if parseErrors%10000 == 0 {
				log.Printf("Warning: failed to parse record at line %d: %v", lineCount+1, err)
			}
			continue
		}

		batch = append(batch, geoname)
		lineCount++

		// Send batch if full
		if len(batch) >= p.batchSize {
			select {
			case p.batchChan <- batch:
				batch = make([]*domain.Geoname, 0, p.batchSize)
			case <-ctx.Done():
				return processed, ctx.Err()
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return processed, fmt.Errorf("error reading file: %w", err)
	}

	// Send final batch
	if len(batch) > 0 {
		select {
		case p.batchChan <- batch:
		case <-ctx.Done():
			return processed, ctx.Err()
		}
	}

	// Wait for all batches to be processed
	close(p.batchChan)

	// Check for errors from consumer
	select {
	case err := <-errChan:
		if err != nil {
			return processed, err
		}
	case <-ctx.Done():
		return processed, ctx.Err()
	}

	if parseErrors > 0 {
		log.Printf("Completed geonames with %d parse errors (%.2f%%)",
			parseErrors, float64(parseErrors)/float64(lineCount+parseErrors)*100)
	}

	return lineCount, nil
}

// parseGeonameFromFields parses fields into a Geoname struct
func (p *GeonameParser) parseGeonameFromFields(fields []string, lineNum int64) (*domain.Geoname, error) {
	if len(fields) < 19 {
		return nil, fmt.Errorf("line %d: insufficient fields: %d", lineNum, len(fields))
	}

	// Очищаем поля от пробелов
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}

	// Parse ID
	id, err := p.ParseInt(fields[0])
	if err != nil {
		return nil, fmt.Errorf("line %d: invalid ID '%s': %w", lineNum, fields[0], err)
	}

	// Parse coordinates - могут быть пустыми
	var lat, lon float64

	if fields[4] != "" && fields[4] != "\\N" {
		lat, err = p.ParseFloat(fields[4])
		if err != nil {
			// Не фатально, просто логируем и ставим 0
			log.Printf("Debug line %d: invalid latitude '%s' for ID %d", lineNum, fields[4], id)
			lat = 0
		}
	}

	if fields[5] != "" && fields[5] != "\\N" {
		lon, err = p.ParseFloat(fields[5])
		if err != nil {
			log.Printf("Debug line %d: invalid longitude '%s' for ID %d", lineNum, fields[5], id)
			lon = 0
		}
	}

	// Parse numeric fields
	population, _ := p.ParseInt(fields[14])
	dem, _ := p.ParseInt(fields[16])

	// Parse elevation
	var elevation *int
	if fields[15] != "" && fields[15] != "0" && fields[15] != "\\N" {
		elev, err := p.ParseInt(fields[15])
		if err == nil && elev > 0 {
			e := int(elev)
			elevation = &e
		}
	}

	// Parse modification date
	var modDate time.Time
	if fields[18] != "" && fields[18] != "\\N" {
		modDate, _ = p.ParseDate(fields[18])
	}

	// Create geoname
	geoname := &domain.Geoname{
		ID:               id,
		Name:             fields[1],
		ASCIIName:        fields[2],
		AlternateNames:   splitAlternateNames(fields[3]),
		Latitude:         lat,
		Longitude:        lon,
		FeatureClass:     fields[6],
		FeatureCode:      fields[7],
		CountryCode:      fields[8],
		CC2:              splitComma(fields[9]),
		Admin1Code:       fields[10],
		Admin2Code:       fields[11],
		Admin3Code:       fields[12],
		Admin4Code:       fields[13],
		Population:       population,
		Elevation:        elevation,
		DEM:              int(dem),
		Timezone:         fields[17],
		ModificationDate: modDate,
	}
	// Calculate geohash
	geoname.CalculateGeohash(p.geohashLevel)

	return geoname, nil
}

// startConsumer consumes batches and inserts into Manticore
func (p *GeonameParser) startConsumer(ctx context.Context, client *manticore.ManticoreClient, errChan chan error, processed *int64) {
	batchCount := 0
	for batch := range p.batchChan {
		select {
		case <-ctx.Done():
			errChan <- ctx.Err()
			return
		default:
			// Пытаемся вставить batch, если ошибка - логируем и продолжаем со следующим
			if err := client.BulkInsertGeonames(ctx, batch); err != nil {
				log.Printf("Warning: failed to insert batch of %d records: %v", len(batch), err)
				// Можно сохранить проблемный batch для анализа
				continue
			}
			*processed += int64(len(batch))
			batchCount++
			if batchCount%10 == 0 {
				log.Printf("Inserted %d records so far...", *processed)
			}
		}
	}
	errChan <- nil
}

// parseGeoname parses a TSV record into a Geoname struct
func (p *GeonameParser) parseGeoname(record []string) (*domain.Geoname, error) {
	// В GeoNames файле всегда 19 полей, но пустые поля могут быть пропущены
	// Нормализуем запись до 19 полей
	normalized := make([]string, 19)
	for i := 0; i < 19; i++ {
		if i < len(record) {
			normalized[i] = strings.TrimSpace(record[i])
		} else {
			normalized[i] = ""
		}
	}

	// Parse ID
	id, err := p.ParseInt(normalized[0])
	if err != nil {
		return nil, fmt.Errorf("invalid ID '%s' at field 0: %w", normalized[0], err)
	}

	// Parse coordinates - могут быть пустыми для некоторых типов объектов
	var lat, lon float64

	if normalized[4] != "" && normalized[4] != "\\N" {
		lat, err = p.ParseFloat(normalized[4])
		if err != nil {
			log.Printf("Warning: invalid latitude '%s' for ID %d: %v", normalized[4], id, err)
			lat = 0
		}
	}

	if normalized[5] != "" && normalized[5] != "\\N" {
		lon, err = p.ParseFloat(normalized[5])
		if err != nil {
			log.Printf("Warning: invalid longitude '%s' for ID %d: %v", normalized[5], id, err)
			lon = 0
		}
	}

	// Parse population
	population, _ := p.ParseInt(normalized[14])

	// Parse DEM
	dem, _ := p.ParseInt(normalized[16])

	// Parse elevation (optional)
	var elevation *int
	if elevStr := normalized[15]; elevStr != "" && elevStr != "0" && elevStr != "\\N" {
		elev, err := p.ParseInt(elevStr)
		if err == nil && elev > 0 {
			e := int(elev)
			elevation = &e
		}
	}

	// Parse modification date
	var modDate time.Time
	if dateStr := normalized[18]; dateStr != "" && dateStr != "\\N" {
		modDate, _ = p.ParseDate(dateStr)
	}

	// Create geoname
	geoname := &domain.Geoname{
		ID:               id,
		Name:             normalized[1],
		ASCIIName:        normalized[2],
		AlternateNames:   splitAlternateNames(normalized[3]),
		Latitude:         lat,
		Longitude:        lon,
		FeatureClass:     normalized[6],
		FeatureCode:      normalized[7],
		CountryCode:      normalized[8],
		CC2:              splitComma(normalized[9]),
		Admin1Code:       normalized[10],
		Admin2Code:       normalized[11],
		Admin3Code:       normalized[12],
		Admin4Code:       normalized[13],
		Population:       population,
		Elevation:        elevation,
		DEM:              int(dem),
		Timezone:         normalized[17],
		ModificationDate: modDate,
	}

	return geoname, nil
}

// Вспомогательные функции
func safeString(record []string, index int) string {
	if index < len(record) {
		return strings.TrimSpace(record[index])
	}
	return ""
}

func splitComma(s string) []string {
	if s == "" || s == "\\N" {
		return []string{}
	}
	return strings.Split(s, ",")
}

func splitAlternateNames(s string) []string {
	if s == "" || s == "\\N" {
		return []string{}
	}
	// Удаляем возможные кавычки и лишние пробелы
	s = strings.Trim(s, "\"")
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
