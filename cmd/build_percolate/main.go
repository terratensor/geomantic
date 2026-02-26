package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/terratensor/geomantic/internal/adapters/repositories/manticore"
	"github.com/terratensor/geomantic/internal/app/services"
	"github.com/terratensor/geomantic/internal/config"
)

func main() {
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	client, err := manticore.NewClient(cfg.ManticoreHost, cfg.ManticorePort)
	if err != nil {
		log.Fatalf("Failed to create manticore client: %v", err)
	}

	builder := services.NewPercolateBuilder(cfg, client)

	if err := builder.Build(ctx); err != nil {
		log.Fatalf("Build failed: %v", err)
	}
	log.Println("Percolate table built successfully!")
}
