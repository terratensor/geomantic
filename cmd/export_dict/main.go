package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/app/services"
	"github.com/terratensor/geomantic/internal/config"
	"github.com/terratensor/geomantic/internal/core/ports"
)

func main() {
	// Парсим флаги командной строки
	var (
		outputPath string
		format     string
	)

	flag.StringVar(&outputPath, "output", "", "output file path (default: export/geoname_dict_YYYYMMDD_HHMMSS.csv)")
	flag.StringVar(&format, "format", "csv", "export format (csv, json, xml)")
	flag.Parse()

	// Загружаем конфигурацию
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Создаём контекст
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Создаём Manticore клиент
	client, err := manticore.NewClient(cfg.ManticoreHost, cfg.ManticorePort)
	if err != nil {
		log.Fatalf("Failed to create manticore client: %v", err)
	}

	// Создаём сервис экспорта
	exportService := services.NewExportService(cfg, client)

	// Определяем путь для экспорта
	exportPath, err := getExportPath(outputPath, format)
	if err != nil {
		log.Fatalf("Failed to create export path: %v", err)
	}

	// Создаём директорию если не существует
	exportDir := filepath.Dir(exportPath)
	if err := os.MkdirAll(exportDir, 0755); err != nil {
		log.Fatalf("Failed to create export directory: %v", err)
	}

	// Настройки экспорта
	options := ports.ExportOptions{
		Format:        ports.ExportFormat(format),
		FilePath:      exportPath,
		IncludeHeader: true,
		Delimiter:     ',',
		BatchSize:     10000,
	}

	// Запускаем экспорт
	log.Printf("Starting geoname_dict export to: %s", exportPath)
	if err := exportService.ExportGeonameDict(ctx, options); err != nil {
		log.Fatalf("Export failed: %v", err)
	}

	log.Printf("Export completed successfully: %s", exportPath)
}

// getExportPath возвращает путь для экспорта
func getExportPath(outputPath, format string) (string, error) {
	if outputPath != "" {
		// Используем указанный путь
		return outputPath, nil
	}

	// Создаём путь по умолчанию
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("geoname_dict_%s.%s", timestamp, format)

	// Путь по умолчанию: export/geoname_dict_20250224_143022.csv
	defaultPath := filepath.Join("export", filename)

	// Получаем абсолютный путь для ясности
	absPath, err := filepath.Abs(defaultPath)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path: %w", err)
	}

	return absPath, nil
}
