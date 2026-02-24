package ports

import (
	"context"
	"io"
)

type ExportFormat string

const (
	FormatCSV  ExportFormat = "csv"
	FormatJSON ExportFormat = "json"
	FormatXML  ExportFormat = "xml"
)

type ExportOptions struct {
	Format        ExportFormat
	FilePath      string
	IncludeHeader bool
	Delimiter     rune // для CSV
	PrettyPrint   bool // для JSON/XML
	BatchSize     int  // количество записей для потоковой записи
}

type Exporter interface {
	Export(ctx context.Context, options ExportOptions) error
}

// Writer interface for different formats
type RecordWriter interface {
	WriteHeader(columns []string) error
	WriteRecord(record map[string]interface{}) error
	Close() error
}

// Factory for creating writers
type WriterFactory interface {
	CreateWriter(w io.Writer, options ExportOptions) (RecordWriter, error)
}
