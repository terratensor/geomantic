package pipeline

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/config"
	"github.com/terratensor/geomantic/internal/core/domain"
)

// GeonameParser handles parsing of allCountries.txt
type GeonameParser struct {
	*BaseParser
	batchChan chan []*domain.Geoname
}

func NewGeonameParser(cfg *config.Config) *GeonameParser {
	return &GeonameParser{
		BaseParser: NewBaseParser(cfg),
		batchChan:  make(chan []*domain.Geoname, cfg.ChannelBufferSize),
	}
}

// ProcessFile processes the geonames file and imports into Manticore
func (p *GeonameParser) ProcessFile(ctx context.Context, filePath string, client *manticore.ManticoreClient) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Create progress bar
	bar, err := p.ProgressBar(file, fmt.Sprintf("Processing %s", filePath))
	if err != nil {
		return 0, err
	}

	// Start worker goroutines
	errChan := make(chan error, p.workers)
	var processed int64
	var parseErrors int64

	// Start consumer goroutine
	go p.startConsumer(ctx, client, errChan, &processed)

	// Create TSV reader with more flexible settings
	reader := p.TSVReader(file)
	reader.FieldsPerRecord = -1 // Разрешить переменное количество полей

	// Read and parse records
	batch := make([]*domain.Geoname, 0, p.batchSize)
	var lineCount int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Пропускаем проблемные строки но продолжаем
			parseErrors++
			if parseErrors%1000 == 0 {
				log.Printf("Warning: %d parse errors so far, last error: %v", parseErrors, err)
			}
			continue
		}

		// Update progress bar based on bytes read (approximate)
		bar.Add(len(strings.Join(record, "\t")) + 1)

		// Parse geoname from record
		geoname, err := p.parseGeoname(record)
		if err != nil {
			parseErrors++
			if parseErrors%1000 == 0 {
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
		log.Printf("Completed with %d parse errors", parseErrors)
	}

	return lineCount, nil
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
	// В GeoNames файле может быть от 4 до 19 полей
	// Минимально нужно: id, name, latitude, longitude
	if len(record) < 5 {
		return nil, fmt.Errorf("record too short: %d fields", len(record))
	}

	// Parse ID (обязательное поле)
	id, err := p.ParseInt(record[0])
	if err != nil {
		return nil, fmt.Errorf("invalid ID '%s': %w", record[0], err)
	}

	// Parse coordinates (обязательные поля)
	lat, err := p.ParseFloat(safeString(record, 4))
	if err != nil {
		return nil, fmt.Errorf("invalid latitude: %w", err)
	}

	lon, err := p.ParseFloat(safeString(record, 5))
	if err != nil {
		return nil, fmt.Errorf("invalid longitude: %w", err)
	}

	// Опциональные поля с значениями по умолчанию
	population, _ := p.ParseInt(safeString(record, 14))
	dem, _ := p.ParseInt(safeString(record, 16))

	// Parse elevation (optional)
	var elevation *int
	if elevStr := safeString(record, 15); elevStr != "" && elevStr != "0" && elevStr != "\\N" {
		elev, err := p.ParseInt(elevStr)
		if err == nil && elev > 0 {
			e := int(elev)
			elevation = &e
		}
	}

	// Parse modification date
	var modDate time.Time
	if dateStr := safeString(record, 18); dateStr != "" && dateStr != "\\N" {
		modDate, _ = p.ParseDate(dateStr)
	}

	// Create geoname with safe defaults for missing fields
	geoname := &domain.Geoname{
		ID:               id,
		Name:             safeString(record, 1),
		ASCIIName:        safeString(record, 2),
		AlternateNames:   splitAlternateNames(safeString(record, 3)),
		Latitude:         lat,
		Longitude:        lon,
		FeatureClass:     safeString(record, 6),
		FeatureCode:      safeString(record, 7),
		CountryCode:      safeString(record, 8),
		CC2:              splitComma(safeString(record, 9)),
		Admin1Code:       safeString(record, 10),
		Admin2Code:       safeString(record, 11),
		Admin3Code:       safeString(record, 12),
		Admin4Code:       safeString(record, 13),
		Population:       population,
		Elevation:        elevation,
		DEM:              int(dem),
		Timezone:         safeString(record, 17),
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
