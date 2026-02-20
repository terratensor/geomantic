package services

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
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

	// Распаковываем если нужно
	if filepath.Ext(filename) == ".zip" {
		localPath, err = i.downloader.ExtractZip(localPath)
		if err != nil {
			return fmt.Errorf("failed to extract: %w", err)
		}
	}

	// Определяем тип файла и соответствующий парсер
	var parser pipeline.Parser

	switch filename {
	case "allCountries.zip":
		parser = pipeline.NewGeonameParser(i.cfg)
	case "alternateNamesV2.zip":
		parser = pipeline.NewAlternateNameParser(i.cfg)
	case "hierarchy.zip":
		parser = pipeline.NewHierarchyParser(i.cfg)
	default:
		return fmt.Errorf("unknown file type: %s", filename)
	}

	// Парсим и импортируем
	start := time.Now()
	count, err := parser.ProcessFile(ctx, localPath, i.manticore)
	if err != nil {
		return fmt.Errorf("failed to process: %w", err)
	}

	log.Printf("Imported %d records from %s in %v", count, filename, time.Since(start))
	return nil
}

func (i *Importer) buildHierarchy(ctx context.Context) error {
	// TODO: Реализовать построение иерархии
	// 1. Загрузить admin коды из admin1CodesASCII.txt и admin2Codes.txt
	// 2. Построить связи на основе admin кодов
	// 3. Объединить с данными из hierarchy.zip
	// 4. Обновить parent_id и hierarchy_path в geonames
	log.Println("Hierarchy building not yet implemented")
	return nil
}
