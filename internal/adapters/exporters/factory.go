package exporters

import (
	"fmt"
	"io"
	"os"

	"github.com/terratensor/geomantic/internal/core/ports"
)

type WriterFactory struct{}

func NewWriterFactory() *WriterFactory {
	return &WriterFactory{}
}

func (f *WriterFactory) CreateWriter(w io.Writer, options ports.ExportOptions) (ports.RecordWriter, error) {
	switch options.Format {
	case ports.FormatCSV:
		return NewCSVWriter(w, options)
	case ports.FormatJSON:
		// TODO: Implement JSON writer
		return nil, fmt.Errorf("JSON format not implemented yet")
	case ports.FormatXML:
		// TODO: Implement XML writer
		return nil, fmt.Errorf("XML format not implemented yet")
	default:
		return nil, fmt.Errorf("unsupported format: %s", options.Format)
	}
}

// CreateFileWriter создает writer для файла
func (f *WriterFactory) CreateFileWriter(filePath string, options ports.ExportOptions) (ports.RecordWriter, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	writer, err := f.CreateWriter(file, options)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Возвращаем composit writer который закроет и файл
	return &fileWriter{
		RecordWriter: writer,
		file:         file,
	}, nil
}

type fileWriter struct {
	ports.RecordWriter
	file *os.File
}

func (w *fileWriter) Close() error {
	if err := w.RecordWriter.Close(); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}
