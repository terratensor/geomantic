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
)

// HierarchyParser handles parsing of hierarchy.txt
type HierarchyParser struct {
	*BaseParser
	batchChan chan []map[string]interface{}
}

func NewHierarchyParser(cfg *config.Config) *HierarchyParser {
	return &HierarchyParser{
		BaseParser: NewBaseParser(cfg),
		batchChan:  make(chan []map[string]interface{}, cfg.ChannelBufferSize),
	}
}

// ProcessFile processes the hierarchy file and imports into Manticore
func (p *HierarchyParser) ProcessFile(ctx context.Context, filePath string, client *manticore.ManticoreClient) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	log.Printf("Processing hierarchy file: %s", filepath.Base(filePath))

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

	// Create TSV reader
	reader := p.TSVReader(file)
	reader.FieldsPerRecord = -1
	reader.Comment = 0

	// Read and parse records
	batch := make([]map[string]interface{}, 0, p.batchSize)
	var lineCount int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			parseErrors++
			if parseErrors%1000 == 0 {
				log.Printf("Warning: %d parse errors in hierarchy, last error: %v", parseErrors, err)
			}
			continue
		}

		// Update progress bar
		bar.Add(len(strings.Join(record, "\t")) + 1)

		// Skip empty lines
		if len(record) < 2 {
			parseErrors++
			continue
		}

		// Parse hierarchy relation into map for Manticore (без id)
		doc, err := p.parseHierarchyToMap(record)
		if err != nil {
			parseErrors++
			if parseErrors%1000 == 0 {
				log.Printf("Warning: failed to parse hierarchy at line %d: %v", lineCount+1, err)
			}
			continue
		}

		batch = append(batch, doc)
		lineCount++

		// Send batch if full
		if len(batch) >= p.batchSize {
			select {
			case p.batchChan <- batch:
				batch = make([]map[string]interface{}, 0, p.batchSize)
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
		log.Printf("Completed hierarchy with %d parse errors", parseErrors)
	}

	return lineCount, nil
}

// startConsumer consumes batches and inserts into Manticore
func (p *HierarchyParser) startConsumer(ctx context.Context, client *manticore.ManticoreClient, errChan chan error, processed *int64) {
	batchCount := 0
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
			batchCount++
			if batchCount%10 == 0 {
				log.Printf("Inserted %d hierarchy records so far...", *processed)
			}
		}
	}
	errChan <- nil
}

// parseHierarchyToMap parses a TSV record into a map for Manticore (без id)
func (p *HierarchyParser) parseHierarchyToMap(record []string) (map[string]interface{}, error) {
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
		relationType = strings.TrimSpace(record[2])
	}

	// Create document WITHOUT id - Manticore сгенерирует сама
	doc := map[string]interface{}{
		"parent_id":       parentID,
		"child_id":        childID,
		"relation_type":   relationType,
		"is_admin":        relationType == "ADM",
		"is_user_defined": relationType != "ADM" && relationType != "",
	}

	return doc, nil
}
