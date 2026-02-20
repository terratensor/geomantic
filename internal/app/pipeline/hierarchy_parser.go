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

// HierarchyParser handles parsing of hierarchy.txt
type HierarchyParser struct {
	*BaseParser
	batchChan chan []*domain.HierarchyRelation
}

func NewHierarchyParser(cfg *config.Config) *HierarchyParser {
	return &HierarchyParser{
		BaseParser: NewBaseParser(cfg),
		batchChan:  make(chan []*domain.HierarchyRelation, cfg.ChannelBufferSize),
	}
}

// ProcessFile processes the hierarchy file and imports into Manticore
func (p *HierarchyParser) ProcessFile(ctx context.Context, filePath string, client *manticore.ManticoreClient) (int64, error) {
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
	batch := make([]*domain.HierarchyRelation, 0, p.batchSize)
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

		// Skip empty lines
		if len(record) == 0 || (len(record) == 1 && record[0] == "") {
			continue
		}

		// Parse hierarchy relation
		relation, err := p.parseHierarchyRelation(record)
		if err != nil {
			fmt.Printf("Warning: failed to parse record at line %d: %v\n", lineCount+1, err)
			continue
		}

		batch = append(batch, relation)
		lineCount++

		// Send batch if full
		if len(batch) >= p.batchSize {
			select {
			case p.batchChan <- batch:
				batch = make([]*domain.HierarchyRelation, 0, p.batchSize)
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
func (p *HierarchyParser) startConsumer(ctx context.Context, client *manticore.ManticoreClient, errChan chan error, processed *int64) {
	for batch := range p.batchChan {
		select {
		case <-ctx.Done():
			errChan <- ctx.Err()
			return
		default:
			if err := client.BulkInsertHierarchyRelations(ctx, batch); err != nil {
				errChan <- fmt.Errorf("failed to insert batch: %w", err)
				return
			}
			*processed += int64(len(batch))
		}
	}
	errChan <- nil
}

// parseHierarchyRelation parses a TSV record into a HierarchyRelation struct
func (p *HierarchyParser) parseHierarchyRelation(record []string) (*domain.HierarchyRelation, error) {
	if len(record) < 2 {
		return nil, fmt.Errorf("invalid record length: %d", len(record))
	}

	// Parse parent ID
	parentID, err := p.ParseInt(record[0])
	if err != nil {
		return nil, fmt.Errorf("invalid parent ID: %w", err)
	}

	// Parse child ID
	childID, err := p.ParseInt(record[1])
	if err != nil {
		return nil, fmt.Errorf("invalid child ID: %w", err)
	}

	// Parse relation type (optional)
	relationType := ""
	if len(record) >= 3 {
		relationType = record[2]
	}

	// Create hierarchy relation
	relation := &domain.HierarchyRelation{
		ParentID:     parentID,
		ChildID:      childID,
		RelationType: relationType,
	}

	return relation, nil
}
