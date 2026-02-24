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
# 	docker compose -f docker/docker-compose.yml down -v

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

# Построение иерархии
hierarchy:
	go run cmd/build_hierarchy/main.go

# Построение только путей (без перестроения иерархии)
paths:
	go run cmd/build_paths/main.go

# Построение словаря имён
name-dict:
	go run cmd/build_name_dict/main.go

# Полный цикл: импорт + иерархия + пути
full:
	make import
	make hierarchy
	make paths	

# Проверка статуса иерархии
hierarchy-status:
	@echo "Top level objects (continents and countries):"
	@curl -s -X POST http://localhost:9309/sql \
		-H "Content-Type: text/plain" \
		-d "SELECT id, name, feature_code FROM geonames WHERE feature_code IN ('CONT', 'PCLI') LIMIT 10" | jq .
	
	@echo "\nHierarchy relations count:"
	@curl -s -X POST http://localhost:9309/sql \
		-H "Content-Type: text/plain" \
		-d "SELECT COUNT(*) FROM hierarchy" | jq .

# Очистка только таблиц (файлы остаются)
drop-tables:
	go run cmd/drop_tables/main.go

# Полная перезагрузка с нуля (таблицы + импорт)
rebuild:
	make drop-tables
	make import
	make hierarchy
	make paths

# Быстрая перезагрузка только geonames с новыми полями
rebuild-geonames:
	curl -X POST http://localhost:9309/sql -H "Content-Type: text/plain" -d "DROP TABLE IF EXISTS geonames"
	curl -X POST http://localhost:9309/sql -H "Content-Type: text/plain" -d "DROP TABLE IF EXISTS hierarchy_paths"
	make import
	make hierarchy
	make paths		

