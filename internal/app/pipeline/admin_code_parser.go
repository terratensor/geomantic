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
)

// AdminCodeParser парсит admin коды
type AdminCodeParser struct {
	*BaseParser
	batchChan chan []map[string]interface{}
	level     int // 1,2,5
}

func NewAdminCode1Parser(cfg *config.Config) *AdminCodeParser {
	return &AdminCodeParser{
		BaseParser: NewBaseParser(cfg),
		batchChan:  make(chan []map[string]interface{}, cfg.ChannelBufferSize),
		level:      1,
	}
}

func NewAdminCode2Parser(cfg *config.Config) *AdminCodeParser {
	return &AdminCodeParser{
		BaseParser: NewBaseParser(cfg),
		batchChan:  make(chan []map[string]interface{}, cfg.ChannelBufferSize),
		level:      2,
	}
}

func NewAdminCode5Parser(cfg *config.Config) *AdminCodeParser {
	return &AdminCodeParser{
		BaseParser: NewBaseParser(cfg),
		batchChan:  make(chan []map[string]interface{}, cfg.ChannelBufferSize),
		level:      5,
	}
}

// ProcessFile обрабатывает файл с admin кодами
func (p *AdminCodeParser) ProcessFile(ctx context.Context, filePath string, client *manticore.ManticoreClient) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	log.Printf("Processing admin codes level %d from: %s", p.level, filepath.Base(filePath))

	// Создаем прогресс бар
	bar, err := p.ProgressBar(file, fmt.Sprintf("Processing admin%d", p.level))
	if err != nil {
		return 0, err
	}

	// Запускаем consumer
	errChan := make(chan error, p.workers)
	var processed int64
	go p.startConsumer(ctx, client, errChan, &processed)

	scanner := bufio.NewScanner(file)
	batch := make([]map[string]interface{}, 0, p.batchSize)
	var lineCount int64

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		bar.Add(len(line) + 1)

		var doc map[string]interface{}

		switch p.level {
		case 1:
			doc, err = p.parseAdmin1(line)
		case 2:
			doc, err = p.parseAdmin2(line)
		case 5:
			doc, err = p.parseAdmin5(line)
		}

		if err != nil {
			log.Printf("Warning: failed to parse line %d: %v", lineCount+1, err)
			continue
		}

		batch = append(batch, doc)
		lineCount++

		if len(batch) >= p.batchSize {
			select {
			case p.batchChan <- batch:
				batch = make([]map[string]interface{}, 0, p.batchSize)
			case <-ctx.Done():
				return processed, ctx.Err()
			}
		}
	}

	// Финальный батч
	if len(batch) > 0 {
		select {
		case p.batchChan <- batch:
		case <-ctx.Done():
			return processed, ctx.Err()
		}
	}

	close(p.batchChan)

	select {
	case err := <-errChan:
		if err != nil {
			return processed, err
		}
	case <-ctx.Done():
		return processed, ctx.Err()
	}

	if err := scanner.Err(); err != nil {
		return processed, fmt.Errorf("error reading file: %w", err)
	}

	return lineCount, nil
}

// startConsumer вставляет батчи в Manticore
func (p *AdminCodeParser) startConsumer(ctx context.Context, client *manticore.ManticoreClient, errChan chan error, processed *int64) {
	for batch := range p.batchChan {
		select {
		case <-ctx.Done():
			errChan <- ctx.Err()
			return
		default:
			if err := client.BulkInsertAdminCodes(ctx, batch); err != nil {
				errChan <- fmt.Errorf("failed to insert batch: %w", err)
				return
			}
			*processed += int64(len(batch))
		}
	}
	errChan <- nil
}

// parseAdmin1 парсит строку из admin1CodesASCII.txt
// Формат: code, name, asciiname, geonameId
// Пример: AD.02	Canillo	Canillo	304156
func (p *AdminCodeParser) parseAdmin1(line string) (map[string]interface{}, error) {
	fields := strings.Split(line, "\t")
	if len(fields) < 4 {
		return nil, fmt.Errorf("invalid format: %s", line)
	}

	code := strings.TrimSpace(fields[0])
	name := strings.TrimSpace(fields[1])
	asciiName := strings.TrimSpace(fields[2])

	geonameID, err := p.ParseInt(fields[3])
	if err != nil {
		return nil, fmt.Errorf("invalid geonameId: %s", fields[3])
	}

	// Разбираем код на части (например, "AD.02" -> parent "AD")
	parts := strings.Split(code, ".")
	parentCode := ""
	if len(parts) > 1 {
		parentCode = parts[0]
	}

	return map[string]interface{}{
		"id":          geonameID,
		"code":        code,
		"name":        name,
		"ascii_name":  asciiName,
		"geoname_id":  geonameID,
		"level":       1,
		"parent_code": parentCode,
	}, nil
}

// parseAdmin2 парсит строку из admin2Codes.txt
// Формат: concatenated codes, name, asciiname, geonameId
// Пример: AD.02.01	Canillo	Canillo	304156
func (p *AdminCodeParser) parseAdmin2(line string) (map[string]interface{}, error) {
	fields := strings.Split(line, "\t")
	if len(fields) < 4 {
		return nil, fmt.Errorf("invalid format: %s", line)
	}

	code := strings.TrimSpace(fields[0])
	name := strings.TrimSpace(fields[1])
	asciiName := strings.TrimSpace(fields[2])

	geonameID, err := p.ParseInt(fields[3])
	if err != nil {
		return nil, fmt.Errorf("invalid geonameId: %s", fields[3])
	}

	// Разбираем код на части (например, "AD.02.01" -> parent "AD.02")
	parts := strings.Split(code, ".")
	parentCode := ""
	if len(parts) > 2 {
		parentCode = strings.Join(parts[:len(parts)-1], ".")
	}

	return map[string]interface{}{
		"id":          geonameID,
		"code":        code,
		"name":        name,
		"ascii_name":  asciiName,
		"geoname_id":  geonameID,
		"level":       2,
		"parent_code": parentCode,
	}, nil
}

// parseAdmin5 парсит строку из adminCode5.txt
// Формат: geonameId, adm5code
// Пример: 304156	01.02
func (p *AdminCodeParser) parseAdmin5(line string) (map[string]interface{}, error) {
	fields := strings.Split(line, "\t")
	if len(fields) < 2 {
		return nil, fmt.Errorf("invalid format: %s", line)
	}

	geonameID, err := p.ParseInt(fields[0])
	if err != nil {
		return nil, fmt.Errorf("invalid geonameId: %s", fields[0])
	}

	code := strings.TrimSpace(fields[1])

	return map[string]interface{}{
		"id":         geonameID,
		"code":       code,
		"geoname_id": geonameID,
		"level":      5,
	}, nil
}
