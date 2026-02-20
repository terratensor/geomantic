package pipeline

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/config"
)

// Parser defines interface for all parsers
type Parser interface {
	ProcessFile(ctx context.Context, filePath string, client *manticore.ManticoreClient) (int64, error)
}

// BaseParser contains common functionality for all parsers
type BaseParser struct {
	cfg       *config.Config
	batchSize int
	workers   int
}

func NewBaseParser(cfg *config.Config) *BaseParser {
	return &BaseParser{
		cfg:       cfg,
		batchSize: cfg.BatchSize,
		workers:   cfg.WorkersCount,
	}
}

// TSVReader creates a TSV reader with proper settings for GeoNames format
func (p *BaseParser) TSVReader(file *os.File) *csv.Reader {
	reader := csv.NewReader(bufio.NewReader(file))
	reader.Comma = '\t'
	reader.Comment = '#' // Игнорируем комментарии
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1 // Разрешаем переменное количество полей
	reader.ReuseRecord = true
	return reader
}

// ProgressBar creates a progress bar for file processing
func (p *BaseParser) ProgressBar(file *os.File, description string) (*progressbar.ProgressBar, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	return progressbar.NewOptions64(
		stat.Size(),
		progressbar.OptionSetDescription(description),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(50),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			fmt.Println()
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
	), nil
}

// ParseDate parses date in yyyy-MM-dd format
func (p *BaseParser) ParseDate(dateStr string) (time.Time, error) {
	if dateStr == "" {
		return time.Time{}, nil
	}
	return time.Parse("2006-01-02", dateStr)
}

// SplitComma splits comma-separated string, handling empty case
func (p *BaseParser) SplitComma(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ",")
}

// ParseInt parses integer, handling empty case and special characters
func (p *BaseParser) ParseInt(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" || s == "\\N" || s == "P" {
		return 0, nil
	}
	var val int64
	_, err := fmt.Sscanf(s, "%d", &val)
	return val, err
}

// ParseFloat parses float, handling empty case
func (p *BaseParser) ParseFloat(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "\\N" {
		return 0, nil
	}
	var val float64
	_, err := fmt.Sscanf(s, "%f", &val)
	if err != nil {
		// Пробуем заменить запятую на точку, если есть
		s = strings.Replace(s, ",", ".", -1)
		_, err = fmt.Sscanf(s, "%f", &val)
	}
	return val, err
}
