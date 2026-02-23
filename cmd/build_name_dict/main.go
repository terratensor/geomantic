package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
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

	// Создаём Manticore клиент
	client, err := manticore.NewClient(cfg.ManticoreHost, cfg.ManticorePort)
	if err != nil {
		log.Fatalf("Failed to create manticore client: %v", err)
	}

	// Создаём построитель словаря
	builder := services.NewNameDictBuilder(cfg, client)

	// Запускаем построение словаря
	log.Println("Starting name dictionary building...")
	if err := builder.BuildDictionary(ctx); err != nil {
		log.Fatalf("Failed to build name dictionary: %v", err)
	}

	log.Println("Name dictionary built successfully!")
}
