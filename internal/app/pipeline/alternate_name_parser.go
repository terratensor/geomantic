package pipeline

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/config"
	"github.com/terratensor/geomantic/internal/core/domain"
)

// AlternateNameParser handles parsing of alternateNamesV2.txt
type AlternateNameParser struct {
	*BaseParser
	batchChan chan []*domain.AlternateName
}

func NewAlternateNameParser(cfg *config.Config) *AlternateNameParser {
	return &AlternateNameParser{
		BaseParser: NewBaseParser(cfg),
		batchChan:  make(chan []*domain.AlternateName, cfg.ChannelBufferSize),
	}
}

// ProcessFile processes the alternate names file and imports into Manticore
func (p *AlternateNameParser) ProcessFile(ctx context.Context, filePath string, client *manticore.ManticoreClient) (int64, error) {
	// Определяем, какой файл обрабатываем
	if strings.Contains(filePath, "iso-languagecodes.txt") {
		log.Printf("Skipping language codes file: %s", filePath)
		return 0, nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	log.Printf("Processing alternate names file: %s", filePath)

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

	// Create TSV reader with flexible settings
	reader := p.TSVReader(file)
	reader.FieldsPerRecord = -1 // Allow variable number of fields
	reader.Comment = 0          // Disable comment skipping for this file

	// Read and parse records
	batch := make([]*domain.AlternateName, 0, p.batchSize)
	var lineCount int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			parseErrors++
			if parseErrors%1000 == 0 {
				log.Printf("Warning: %d parse errors in alternate names, last error: %v", parseErrors, err)
			}
			continue
		}

		// Update progress bar
		bar.Add(len(strings.Join(record, "\t")) + 1)

		// Skip header or invalid records
		if len(record) < 4 {
			parseErrors++
			continue
		}

		// Parse alternate name from record
		altName, err := p.parseAlternateName(record)
		if err != nil {
			parseErrors++
			if parseErrors%10000 == 0 {
				log.Printf("Warning: failed to parse alternate name at line %d: %v", lineCount+1, err)
			}
			continue
		}

		batch = append(batch, altName)
		lineCount++

		// Send batch if full
		if len(batch) >= p.batchSize {
			select {
			case p.batchChan <- batch:
				batch = make([]*domain.AlternateName, 0, p.batchSize)
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
		log.Printf("Completed alternate names with %d parse errors", parseErrors)
	}

	return lineCount, nil
}

// startConsumer consumes batches and inserts into Manticore
func (p *AlternateNameParser) startConsumer(ctx context.Context, client *manticore.ManticoreClient, errChan chan error, processed *int64) {
	for batch := range p.batchChan {
		select {
		case <-ctx.Done():
			errChan <- ctx.Err()
			return
		default:
			if err := client.BulkInsertAlternateNames(ctx, batch); err != nil {
				errChan <- fmt.Errorf("failed to insert batch: %w", err)
				return
			}
			*processed += int64(len(batch))
		}
	}
	errChan <- nil
}

// parseAlternateName parses a TSV record into an AlternateName struct
func (p *AlternateNameParser) parseAlternateName(record []string) (*domain.AlternateName, error) {
	// Alternate names file can have 4 to 11 fields
	if len(record) < 4 {
		return nil, fmt.Errorf("record too short: %d fields", len(record))
	}

	// Parse ID
	id, err := p.ParseInt(record[0])
	if err != nil {
		return nil, fmt.Errorf("invalid ID: %w", err)
	}

	// Parse GeonameID
	geonameID, err := p.ParseInt(record[1])
	if err != nil {
		return nil, fmt.Errorf("invalid geonameID: %w", err)
	}

	// ISO language (may be empty)
	isoLang := ""
	if len(record) > 2 {
		isoLang = strings.TrimSpace(record[2])
	}

	// Alternate name
	altName := ""
	if len(record) > 3 {
		altName = strings.TrimSpace(record[3])
	}

	// Boolean fields (default to false)
	isPreferred := false
	if len(record) > 4 {
		isPreferred = record[4] == "1"
	}

	isShort := false
	if len(record) > 5 {
		isShort = record[5] == "1"
	}

	isColloquial := false
	if len(record) > 6 {
		isColloquial = record[6] == "1"
	}

	isHistoric := false
	if len(record) > 7 {
		isHistoric = record[7] == "1"
	}

	// From/to periods
	var from, to *string
	if len(record) > 8 && record[8] != "" && record[8] != "\\N" {
		from = &record[8]
	}
	if len(record) > 9 && record[9] != "" && record[9] != "\\N" {
		to = &record[9]
	}

	// Create alternate name
	altNameObj := &domain.AlternateName{
		ID:              id,
		GeonameID:       geonameID,
		ISOLanguage:     isoLang,
		AlternateName:   altName,
		IsPreferredName: isPreferred,
		IsShortName:     isShort,
		IsColloquial:    isColloquial,
		IsHistoric:      isHistoric,
		From:            from,
		To:              to,
	}

	return altNameObj, nil
}
