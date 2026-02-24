package exporters

import (
	"encoding/csv"
	"io"
	"strconv"
	"strings"

	"github.com/terratensor/geomantic/internal/core/ports" // правильный импорт
)

type CSVWriter struct {
	writer  *csv.Writer
	options ports.ExportOptions // используем ports.ExportOptions
	columns []string
}

func NewCSVWriter(w io.Writer, options ports.ExportOptions) (*CSVWriter, error) { // ports.ExportOptions
	csvWriter := csv.NewWriter(w)
	if options.Delimiter != 0 {
		csvWriter.Comma = options.Delimiter
	} else {
		csvWriter.Comma = ',' // default
	}

	return &CSVWriter{
		writer:  csvWriter,
		options: options,
		columns: []string{ // определяем здесь, не из export пакета
			"name",
			"geohashes_string",
			"geohashes_uint64",
			"occurrences",
			"first_geoname_id",
		},
	}, nil
}

func (w *CSVWriter) WriteHeader(columns []string) error {
	if !w.options.IncludeHeader {
		return nil
	}
	return w.writer.Write(columns)
}

func (w *CSVWriter) WriteRecord(record map[string]interface{}) error {
	row := make([]string, len(w.columns))

	for i, col := range w.columns {
		val := record[col]

		switch col {
		case "geohashes_uint64":
			// Преобразуем массив uint64 в строку
			if arr, ok := val.([]interface{}); ok {
				strs := make([]string, len(arr))
				for j, v := range arr {
					u64, err := toUint64(v)
					if err != nil {
						strs[j] = "0"
					} else {
						strs[j] = strconv.FormatUint(u64, 10)
					}
				}
				row[i] = strings.Join(strs, ",")
			} else {
				row[i] = ""
			}

		case "occurrences", "first_geoname_id":
			// Числовые поля
			if num, ok := val.(float64); ok {
				row[i] = strconv.FormatInt(int64(num), 10)
			} else {
				row[i] = "0"
			}

		default:
			// Строковые поля
			if s, ok := val.(string); ok {
				row[i] = s
			} else {
				row[i] = ""
			}
		}
	}

	return w.writer.Write(row)
}

func (w *CSVWriter) Close() error {
	w.writer.Flush()
	return w.writer.Error()
}

// Вспомогательная функция для преобразования в uint64
func toUint64(val interface{}) (uint64, error) {
	if val == nil {
		return 0, nil
	}

	switch v := val.(type) {
	case float64:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint64:
		return v, nil
	case int:
		return uint64(v), nil
	default:
		return 0, nil
	}
}
