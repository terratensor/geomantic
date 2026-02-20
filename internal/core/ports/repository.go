package ports

import (
	"context"

	"github.com/terratensor/geomantic/internal/core/domain"
)

// GeonameRepository определяет порт для работы с geonames
type GeonameRepository interface {
	// Базовые операции
	Insert(ctx context.Context, geoname *domain.Geoname) error
	InsertBatch(ctx context.Context, geonames []*domain.Geoname) error
	Update(ctx context.Context, geoname *domain.Geoname) error
	Delete(ctx context.Context, id int64) error
	GetByID(ctx context.Context, id int64) (*domain.Geoname, error)

	// Поиск
	Search(ctx context.Context, query string, filters map[string]interface{}, limit, offset int) ([]*domain.Geoname, int64, error)
	SearchByLocation(ctx context.Context, lat, lon float64, radius float64, limit int) ([]*domain.Geoname, error)

	// Иерархия
	GetChildren(ctx context.Context, parentID int64) ([]*domain.Geoname, error)
	GetParents(ctx context.Context, childID int64) ([]*domain.Geoname, error)

	// Административные операции
	Count(ctx context.Context) (int64, error)
	Truncate(ctx context.Context) error

	// BulkUpdateGeonames обновляет геонимы пачкой
	BulkUpdateGeonames(ctx context.Context, updates []map[string]interface{}) error
}

// AlternateNameRepository определяет порт для работы с альтернативными именами
type AlternateNameRepository interface {
	Insert(ctx context.Context, altName *domain.AlternateName) error
	InsertBatch(ctx context.Context, altNames []*domain.AlternateName) error
	Update(ctx context.Context, altName *domain.AlternateName) error
	Delete(ctx context.Context, id int64) error
	GetByGeonameID(ctx context.Context, geonameID int64) ([]*domain.AlternateName, error)
	GetByLanguage(ctx context.Context, language string, limit, offset int) ([]*domain.AlternateName, int64, error)

	// Поиск
	Search(ctx context.Context, query string, language string, limit int) ([]*domain.AlternateName, error)

	Count(ctx context.Context) (int64, error)
	Truncate(ctx context.Context) error
}

// HierarchyRepository определяет порт для работы с иерархией
type HierarchyRepository interface {
	InsertRelation(ctx context.Context, parentID, childID int64, relationType string) error
	InsertBatchRelations(ctx context.Context, relations []*domain.HierarchyRelation) error
	GetHierarchy(ctx context.Context, nodeID int64) (*domain.HierarchyNode, error)
	GetPath(ctx context.Context, nodeID int64) ([]*domain.Geoname, error)
	GetSubtree(ctx context.Context, rootID int64, depth int) ([]*domain.Geoname, error)

	// Обновление иерархии
	RebuildHierarchy(ctx context.Context) error
	UpdateNodeParents(ctx context.Context, nodeID int64, parentIDs []int64) error
}
