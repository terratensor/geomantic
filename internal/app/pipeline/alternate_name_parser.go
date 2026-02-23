package pipeline

import (
	"bufio"
	"context"
	"fmt"
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

	log.Printf("Processing alternate names file: %s", filepath.Base(filePath))

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
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	batch := make([]*domain.AlternateName, 0, p.batchSize)
	var lineCount int64

	for scanner.Scan() {
		line := scanner.Text()

		// Пропускаем пустые строки и комментарии
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Обновляем прогресс
		bar.Add(len(line) + 1)

		// Разделяем строку вручную, сохраняя пустые поля
		fields := strings.Split(line, "\t")

		// Нормализуем до 11 полей (максимальное количество в alternateNamesV2)
		for len(fields) < 11 {
			fields = append(fields, "")
		}

		// Parse alternate name from fields
		altName, err := p.parseAlternateNameFromFields(fields, lineCount+1)
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
		log.Printf("Completed alternate names with %d parse errors", parseErrors)
	}

	return lineCount, nil
}

// startConsumer consumes batches and inserts into Manticore
func (p *AlternateNameParser) startConsumer(ctx context.Context, client *manticore.ManticoreClient, errChan chan error, processed *int64) {
	batchCount := 0
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
			batchCount++
			if batchCount%100 == 0 {
				log.Printf("Inserted %d alternate names so far...", *processed)
			}
		}
	}
	errChan <- nil
}

// parseAlternateNameFromFields parses fields into an AlternateName struct
func (p *AlternateNameParser) parseAlternateNameFromFields(fields []string, lineNum int64) (*domain.AlternateName, error) {
	// Очищаем поля от пробелов
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}

	// Parse ID
	id, err := p.ParseInt(fields[0])
	if err != nil {
		return nil, fmt.Errorf("line %d: invalid ID '%s': %w", lineNum, fields[0], err)
	}

	// Parse GeonameID
	geonameID, err := p.ParseInt(fields[1])
	if err != nil {
		return nil, fmt.Errorf("line %d: invalid geonameID '%s': %w", lineNum, fields[1], err)
	}

	// ISO language (may be empty) - поле 2
	isoLang := fields[2]

	// Alternate name - поле 3
	altName := fields[3]

	// Boolean fields (поля 4-7)
	isPreferred := false
	if len(fields) > 4 && fields[4] == "1" {
		isPreferred = true
	}

	isShort := false
	if len(fields) > 5 && fields[5] == "1" {
		isShort = true
	}

	isColloquial := false
	if len(fields) > 6 && fields[6] == "1" {
		isColloquial = true
	}

	isHistoric := false
	if len(fields) > 7 && fields[7] == "1" {
		isHistoric = true
	}

	// From/to periods (поля 8-9)
	var from, to *string
	if len(fields) > 8 && fields[8] != "" && fields[8] != "\\N" {
		from = &fields[8]
	}
	if len(fields) > 9 && fields[9] != "" && fields[9] != "\\N" {
		to = &fields[9]
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
