package services

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/terratensor/geomantic/internal/adapters/downloader"
	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/app/pipeline"
	"github.com/terratensor/geomantic/internal/config"
)

type Importer struct {
	cfg        *config.Config
	downloader *downloader.Downloader
	manticore  *manticore.ManticoreClient
}

func NewImporter(cfg *config.Config) *Importer {
	return &Importer{
		cfg: cfg,
	}
}

func (i *Importer) Run(ctx context.Context) error {
	// Инициализация компонентов
	if err := i.initComponents(ctx); err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}

	// Инициализация схемы в Manticore
	log.Println("Initializing database schema...")
	if err := i.manticore.InitSchema(ctx); err != nil {
		return fmt.Errorf("failed to init schema: %w", err)
	}

	// Список файлов для импорта
	files := []string{
		"allCountries.zip",
		"alternateNamesV2.zip",
		"hierarchy.zip",
		"admin1CodesASCII.txt",
		"admin2Codes.txt",
		"adminCode5.zip", // опционально
		// admin коды будем обрабатывать отдельно при построении иерархии
	}

	// Импортируем каждый файл
	for _, file := range files {
		if err := i.importFile(ctx, file); err != nil {
			return fmt.Errorf("failed to import %s: %w", file, err)
		}
	}

	// Построение иерархии
	log.Println("Building hierarchy...")
	if err := i.buildHierarchy(ctx); err != nil {
		return fmt.Errorf("failed to build hierarchy: %w", err)
	}

	return nil
}

func (i *Importer) initComponents(ctx context.Context) error {
	var err error

	// Создаём downloader
	i.downloader = downloader.New(i.cfg)

	// Создаём Manticore клиент
	i.manticore, err = manticore.NewClient(i.cfg.ManticoreHost, i.cfg.ManticorePort)
	if err != nil {
		return fmt.Errorf("failed to create manticore client: %w", err)
	}

	return nil
}

func (i *Importer) importFile(ctx context.Context, filename string) error {
	log.Printf("Processing %s...", filename)

	// Скачиваем файл
	localPath, err := i.downloader.DownloadFile(ctx, filename)
	if err != nil {
		return fmt.Errorf("failed to download: %w", err)
	}

	var filesToProcess []string

	// Распаковываем если нужно
	if filepath.Ext(filename) == ".zip" {
		extractedFiles, err := i.downloader.ExtractZip(localPath)
		if err != nil {
			return fmt.Errorf("failed to extract: %w", err)
		}
		filesToProcess = append(filesToProcess, extractedFiles...)
	} else {
		filesToProcess = append(filesToProcess, localPath)
	}

	// Обрабатываем каждый файл
	for _, filePath := range filesToProcess {
		// Определяем тип файла и соответствующий парсер
		var parser pipeline.Parser

		switch {
		case strings.Contains(filePath, "allCountries"):
			parser = pipeline.NewGeonameParser(i.cfg)
		case strings.Contains(filePath, "alternateNamesV2"):
			parser = pipeline.NewAlternateNameParser(i.cfg)
		case strings.Contains(filePath, "hierarchy"):
			parser = pipeline.NewHierarchyParser(i.cfg)
		default:
			log.Printf("Skipping unknown file: %s", filePath)
			continue
		}

		// Парсим и импортируем
		start := time.Now()
		count, err := parser.ProcessFile(ctx, filePath, i.manticore)
		if err != nil {
			return fmt.Errorf("failed to process %s: %w", filePath, err)
		}

		log.Printf("Imported %d records from %s in %v", count, filepath.Base(filePath), time.Since(start))
	}

	return nil
}

func (i *Importer) buildHierarchy(ctx context.Context) error {
	log.Println("Building hierarchy...")

	builder := NewHierarchyBuilder(i.cfg, i.manticore)
	if err := builder.BuildHierarchy(ctx); err != nil {
		return fmt.Errorf("failed to build hierarchy: %w", err)
	}

	log.Println("Hierarchy built successfully")
	return nil
}
