package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/terratensor/geomantic/internal/app/services"
	"github.com/terratensor/geomantic/internal/config"
)

func main() {
	// Загружаем конфигурацию
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Создаём контекст с отменой для graceful shutdown
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

	// Создаём и запускаем импортер
	importer := services.NewImporter(cfg)

	if err := importer.Run(ctx); err != nil {
		log.Fatalf("Import failed: %v", err)
	}

	fmt.Println("Import completed successfully!")
}
