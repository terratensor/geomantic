package export

import (
	"fmt"
	"strconv"
)

// ToUint64 преобразует значение из Manticore в uint64
func ToUint64(val interface{}) (uint64, error) {
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
	case string:
		return strconv.ParseUint(v, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported type for uint64 conversion: %T", val)
	}
}

// Columns for geoname_dict export
var GeonameDictColumns = []string{
	"name",
	"geohashes_string",
	"geohashes_uint64",
	"occurrences",
	"first_geoname_id",
}
