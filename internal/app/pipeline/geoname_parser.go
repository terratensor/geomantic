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

	// Start consumer goroutine
	go p.startConsumer(ctx, client, errChan, &processed)

	// Create TSV reader
	reader := p.TSVReader(file)

	// Read and parse records
	batch := make([]*domain.Geoname, 0, p.batchSize)
	var lineCount int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return processed, fmt.Errorf("error reading record: %w", err)
		}

		// Update progress bar based on bytes read (approximate)
		bar.Add(len(strings.Join(record, "\t")) + 1)

		// Parse geoname from record
		geoname, err := p.parseGeoname(record)
		if err != nil {
			// Log error but continue processing
			fmt.Printf("Warning: failed to parse record at line %d: %v\n", lineCount+1, err)
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

	return lineCount, nil
}

// startConsumer consumes batches and inserts into Manticore
func (p *GeonameParser) startConsumer(ctx context.Context, client *manticore.ManticoreClient, errChan chan error, processed *int64) {
	for batch := range p.batchChan {
		select {
		case <-ctx.Done():
			errChan <- ctx.Err()
			return
		default:
			if err := client.BulkInsertGeonames(ctx, batch); err != nil {
				errChan <- fmt.Errorf("failed to insert batch: %w", err)
				return
			}
			*processed += int64(len(batch))
		}
	}
	errChan <- nil
}

// parseGeoname parses a TSV record into a Geoname struct
func (p *GeonameParser) parseGeoname(record []string) (*domain.Geoname, error) {
	if len(record) < 19 {
		return nil, fmt.Errorf("invalid record length: %d", len(record))
	}

	// Parse ID
	id, err := p.ParseInt(record[0])
	if err != nil {
		return nil, fmt.Errorf("invalid ID: %w", err)
	}

	// Parse coordinates
	lat, err := p.ParseFloat(record[4])
	if err != nil {
		return nil, fmt.Errorf("invalid latitude: %w", err)
	}

	lon, err := p.ParseFloat(record[5])
	if err != nil {
		return nil, fmt.Errorf("invalid longitude: %w", err)
	}

	// Parse population
	population, err := p.ParseInt(record[14])
	if err != nil {
		return nil, fmt.Errorf("invalid population: %w", err)
	}

	// Parse DEM
	dem, err := p.ParseInt(record[16])
	if err != nil {
		return nil, fmt.Errorf("invalid DEM: %w", err)
	}

	// Parse elevation (optional)
	var elevation *int
	if record[15] != "" && record[15] != "0" {
		elev, err := p.ParseInt(record[15])
		if err == nil {
			e := int(elev)
			elevation = &e
		}
	}

	// Parse modification date
	modDate, err := p.ParseDate(record[18])
	if err != nil {
		return nil, fmt.Errorf("invalid modification date: %w", err)
	}

	// Create geoname
	geoname := &domain.Geoname{
		ID:               id,
		Name:             record[1],
		ASCIIName:        record[2],
		AlternateNames:   p.SplitComma(record[3]),
		Latitude:         lat,
		Longitude:        lon,
		FeatureClass:     record[6],
		FeatureCode:      record[7],
		CountryCode:      record[8],
		CC2:              p.SplitComma(record[9]),
		Admin1Code:       record[10],
		Admin2Code:       record[11],
		Admin3Code:       record[12],
		Admin4Code:       record[13],
		Population:       population,
		Elevation:        elevation,
		DEM:              int(dem),
		Timezone:         record[17],
		ModificationDate: modDate,
	}

	return geoname, nil
}
