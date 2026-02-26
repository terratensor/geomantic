package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	// Manticore
	ManticoreHost        string
	ManticorePort        int
	ManticoreConnTimeout time.Duration

	// Download
	GeonamesBaseURL string
	DownloadTimeout time.Duration
	MaxRetries      int

	// Pipeline
	BatchSize         int
	WorkersCount      int
	ChannelBufferSize int

	// Import
	DataDir       string
	FullImport    bool
	UpdateEnabled bool

	// S2 Geometry
	S2GeohashLevel int // уровень точности геохеша (по умолчанию 9)

	// Language filters for name dictionary
	ExcludeCJK    bool // исключать китайские, японские, корейские иероглифы
	ExcludeArabic bool // исключать арабские символы

	// Name normalization
	NormalizeNames bool // нормализация диакритических знаков

	// Percolate settings
	PercolateBatchSize int
}

func Load() (*Config, error) {
	// Загружаем .env файл если существует
	_ = godotenv.Load()

	cfg := &Config{
		ManticoreHost:        getEnv("MANTICORE_HOST", "localhost"),
		ManticorePort:        getEnvAsInt("MANTICORE_PORT", 9308),
		ManticoreConnTimeout: getEnvAsDuration("MANTICORE_TIMEOUT", 30*time.Second),

		GeonamesBaseURL: getEnv("GEONAMES_BASE_URL", "https://download.geonames.org/export/dump/"),
		DownloadTimeout: getEnvAsDuration("DOWNLOAD_TIMEOUT", 10*time.Minute),
		MaxRetries:      getEnvAsInt("MAX_RETRIES", 3),

		BatchSize:         getEnvAsInt("BATCH_SIZE", 1000),
		WorkersCount:      getEnvAsInt("WORKERS_COUNT", 4),
		ChannelBufferSize: getEnvAsInt("CHANNEL_BUFFER_SIZE", 10000),

		DataDir:       getEnv("DATA_DIR", "./data"),
		FullImport:    getEnvAsBool("FULL_IMPORT", true),
		UpdateEnabled: getEnvAsBool("UPDATE_ENABLED", false),

		S2GeohashLevel: getEnvAsInt("S2_GEOHASH_LEVEL", 9), // 9 уровень по умолчанию

		ExcludeCJK:    getEnvAsBool("EXCLUDE_CJK", true),
		ExcludeArabic: getEnvAsBool("EXCLUDE_ARABIC", true),

		NormalizeNames: getEnvAsBool("NORMALIZE_NAMES", false),

		PercolateBatchSize: getEnvAsInt("PERCOLATE_BATCH_SIZE", 10000),
	}

	return cfg, nil
}

// Вспомогательные функции для получения переменных окружения
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getEnvAsBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}
