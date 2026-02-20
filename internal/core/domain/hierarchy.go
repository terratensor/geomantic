package domain

import "fmt"

type HierarchyRelation struct {
	ParentID     int64
	ChildID      int64
	RelationType string // 'ADM', 'amt', '' (user-defined)
}

type HierarchyNode struct {
	GeonameID   int64
	ParentID    *int64
	Name        string
	Level       int
	Path        []int64  // IDs всех предков
	PathNames   []string // Названия всех предков
	CountryCode string
	FeatureCode string
	Children    []*HierarchyNode
}

type HierarchyPath struct {
	GeonameID int64
	Path      string // "continent/country/region/city" для быстрого поиска
	PathIDs   string // "id1/id2/id3/id4"
	Depth     int
}

type AdminCode struct {
	Code       string // "AD.02" или "AD.02.01"
	Name       string
	ASCIIName  string
	GeonameID  int64
	Level      int // 1,2,3,4,5
	ParentCode string
}

func (hr *HierarchyRelation) IsAdmin() bool {
	return hr.RelationType == "ADM"
}

func (hr *HierarchyRelation) IsUserDefined() bool {
	return hr.RelationType == "" || (hr.RelationType != "ADM" && hr.RelationType != "")
}

// String returns a string representation of the relation
func (hr *HierarchyRelation) String() string {
	return fmt.Sprintf("%d -> %d [%s]", hr.ParentID, hr.ChildID, hr.RelationType)
}
