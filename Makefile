.PHONY: init import update clean docker-up docker-down

# Инициализация проекта
init:
	go mod download
	mkdir -p data
	cp .env.example .env

# Полный импорт данных
import:
	go run cmd/geomantic/main.go

# Инкрементальное обновление
update:
	FULL_IMPORT=false go run cmd/geomantic/main.go

# Очистка данных
clean:
	rm -rf data/*
	docker compose -f docker/docker-compose.yml down -v

# Запуск Manticore
docker-up:
	docker compose -f docker/docker-compose.yml up -d

# Остановка Manticore
docker-down:
	docker compose -f docker/docker-compose.yml down

# Логи
logs:
	docker compose -f docker/docker-compose.yml logs -f

# Проверка статуса
status:
	curl http://localhost:9309/cli?sql=SHOW%20TABLES