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
	parser     *pipeline.Parser
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
		"admin1CodesASCII.txt",
		"admin2Codes.txt",
		"hierarchy.zip",
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

	// Создаём парсер
	i.parser = pipeline.NewParser(i.cfg)

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
	if filename[len(filename)-4:] == ".zip" {
		localPath, err = i.downloader.ExtractZip(localPath)
		if err != nil {
			return fmt.Errorf("failed to extract: %w", err)
		}
	}

	// Определяем тип файла и соответствующий обработчик
	var processor func(context.Context, string, *manticore.ManticoreClient) (int64, error)

	switch filename {
	case "allCountries.zip":
		processor = i.processGeonames
	case "alternateNamesV2.zip":
		processor = i.processAlternateNames
	case "hierarchy.zip":
		processor = i.processHierarchy
	default:
		processor = i.processGeneric
	}

	// Парсим и импортируем
	start := time.Now()
	count, err := processor(ctx, localPath, i.manticore)
	if err != nil {
		return fmt.Errorf("failed to process: %w", err)
	}

	log.Printf("Imported %d records from %s in %v", count, filename, time.Since(start))
	return nil
}

// processGeonames обрабатывает файл с геонимами
func (i *Importer) processGeonames(ctx context.Context, filePath string, client *manticore.ManticoreClient) (int64, error) {
	parser := pipeline.NewGeonameParser(i.cfg)
	return parser.ProcessFile(ctx, filePath, client)
}

// processAlternateNames обрабатывает файл с альтернативными именами
func (i *Importer) processAlternateNames(ctx context.Context, filePath string, client *manticore.ManticoreClient) (int64, error) {
	parser := pipeline.NewAlternateNameParser(i.cfg)
	return parser.ProcessFile(ctx, filePath, client)
}

// processHierarchy обрабатывает файл иерархии
func (i *Importer) processHierarchy(ctx context.Context, filePath string, client *manticore.ManticoreClient) (int64, error) {
	parser := pipeline.NewHierarchyParser(i.cfg)
	return parser.ProcessFile(ctx, filePath, client)
}

// processGeneric для остальных файлов (admin коды и т.д.)
func (i *Importer) processGeneric(ctx context.Context, filePath string, client *manticore.ManticoreClient) (int64, error) {
	// Для admin кодов пока просто логируем
	log.Printf("Skipping import for %s - will be used for hierarchy building", filepath.Base(filePath))
	return 0, nil
}

func (i *Importer) buildHierarchy(ctx context.Context) error {
	// TODO: Реализовать построение иерархии
	// 1. Загрузить admin коды
	// 2. Построить связи
	// 3. Обновить parent_id в geonames
	return nil
}
