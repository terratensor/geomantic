package domain

import "fmt"

type HierarchyRelation struct {
	ParentID     int64
	ChildID      int64
	RelationType string // 'ADM', 'amt', '' (user-defined)
}

type HierarchyNode struct {
	Geoname      *Geoname
	Parent       *HierarchyNode
	Children     []*HierarchyNode
	Level        int
	Path         []int64
	RelationType string
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
