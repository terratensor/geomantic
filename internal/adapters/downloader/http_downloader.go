package downloader

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/terratensor/geomantic/internal/config"
)

type Downloader struct {
	client *http.Client
	cfg    *config.Config
}

func New(cfg *config.Config) *Downloader {
	return &Downloader{
		client: &http.Client{
			Timeout: cfg.DownloadTimeout,
		},
		cfg: cfg,
	}
}

func (d *Downloader) DownloadFile(ctx context.Context, filename string) (string, error) {
	url := d.cfg.GeonamesBaseURL + filename
	localPath := filepath.Join(d.cfg.DataDir, filename)

	// Создаём директорию если не существует
	if err := os.MkdirAll(d.cfg.DataDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create data dir: %w", err)
	}

	// Проверяем существует ли уже файл
	if _, err := os.Stat(localPath); err == nil {
		return localPath, nil
	}

	// Создаём файл
	out, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to create file: %w", err)
	}
	defer out.Close()

	// Делаем запрос
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad status: %s", resp.Status)
	}

	// Создаём progress bar
	bar := progressbar.NewOptions64(
		resp.ContentLength,
		progressbar.OptionSetDescription(fmt.Sprintf("Downloading %s", filename)),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(50),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			fmt.Println()
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
	)

	// Копируем с отслеживанием прогресса
	_, err = io.Copy(io.MultiWriter(out, bar), resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to save file: %w", err)
	}

	return localPath, nil
}

// ExtractZip распаковывает zip архив и возвращает список распакованных файлов
func (d *Downloader) ExtractZip(zipPath string) ([]string, error) {
	// Открываем zip архив
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open zip: %w", err)
	}
	defer reader.Close()

	var extractedFiles []string

	for _, zipFile := range reader.File {
		destPath := filepath.Join(d.cfg.DataDir, zipFile.Name)

		// Проверяем существует ли уже распакованный файл
		if _, err := os.Stat(destPath); err == nil {
			extractedFiles = append(extractedFiles, destPath)
			continue
		}

		// Открываем файл в архиве
		rc, err := zipFile.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s in zip: %w", zipFile.Name, err)
		}

		// Создаем выходной файл
		out, err := os.Create(destPath)
		if err != nil {
			rc.Close()
			return nil, fmt.Errorf("failed to create output file %s: %w", destPath, err)
		}

		// Копируем содержимое
		_, err = io.Copy(out, rc)
		rc.Close()
		out.Close()

		if err != nil {
			return nil, fmt.Errorf("failed to extract file %s: %w", zipFile.Name, err)
		}

		extractedFiles = append(extractedFiles, destPath)
		log.Printf("Extracted: %s", destPath)
	}

	return extractedFiles, nil
}
