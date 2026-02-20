package pipeline

import (
	"context"
	"fmt"
	"io"
	"os"
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

	// Start consumer goroutine
	go p.startConsumer(ctx, client, errChan, &processed)

	// Create TSV reader
	reader := p.TSVReader(file)

	// Read and parse records
	batch := make([]*domain.AlternateName, 0, p.batchSize)
	var lineCount int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return processed, fmt.Errorf("error reading record: %w", err)
		}

		// Update progress bar
		bar.Add(len(strings.Join(record, "\t")) + 1)

		// Parse alternate name from record
		altName, err := p.parseAlternateName(record)
		if err != nil {
			fmt.Printf("Warning: failed to parse record at line %d: %v\n", lineCount+1, err)
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
	if len(record) < 11 {
		return nil, fmt.Errorf("invalid record length: %d", len(record))
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

	// Parse boolean fields (columns 6-9 are isPreferredName, isShortName, isColloquial, isHistoric)
	isPreferred := record[5] == "1"
	isShort := record[6] == "1"
	isColloquial := record[7] == "1"
	isHistoric := record[8] == "1"

	// Parse from/to periods (columns 9 and 10)
	var from, to *string
	if len(record) > 9 && record[9] != "" {
		from = &record[9]
	}
	if len(record) > 10 && record[10] != "" {
		to = &record[10]
	}

	// Create alternate name
	altName := &domain.AlternateName{
		ID:              id,
		GeonameID:       geonameID,
		ISOLanguage:     record[2],
		AlternateName:   record[3],
		IsPreferredName: isPreferred,
		IsShortName:     isShort,
		IsColloquial:    isColloquial,
		IsHistoric:      isHistoric,
		From:            from,
		To:              to,
	}

	return altName, nil
}
