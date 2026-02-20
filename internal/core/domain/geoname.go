package domain

import (
	"fmt"
	"strings"
	"time"
)

type FeatureClass string

const (
	FeatureClassA FeatureClass = "A" // country, state, region
	FeatureClassH FeatureClass = "H" // stream, lake
	FeatureClassL FeatureClass = "L" // parks,area
	FeatureClassP FeatureClass = "P" // city, village
	FeatureClassR FeatureClass = "R" // road, railroad
	FeatureClassS FeatureClass = "S" // spot, building, farm
	FeatureClassT FeatureClass = "T" // mountain,hill,rock
	FeatureClassU FeatureClass = "U" // undersea
	FeatureClassV FeatureClass = "V" // forest,heath
)

type Geoname struct {
	ID               int64
	Name             string
	ASCIIName        string
	AlternateNames   []string
	Latitude         float64
	Longitude        float64
	FeatureClass     string
	FeatureCode      string
	CountryCode      string
	CC2              []string
	Admin1Code       string
	Admin2Code       string
	Admin3Code       string
	Admin4Code       string
	Population       int64
	Elevation        *int
	DEM              int
	Timezone         string
	ModificationDate time.Time

	// Иерархия (будет заполняться позже)
	ParentID      *int64
	HierarchyPath string
}

func (g *Geoname) IsPopulatedPlace() bool {
	return strings.HasPrefix(g.FeatureCode, "PPL")
}

func (g *Geoname) IsAdminDivision() bool {
	return strings.HasPrefix(g.FeatureCode, "ADM")
}

func (g *Geoname) IsCountry() bool {
	return g.FeatureCode == "PCLI" || g.FeatureCode == "PCLIX"
}

func (g *Geoname) FullText() string {
	parts := []string{g.Name, g.ASCIIName}
	parts = append(parts, g.AlternateNames...)
	return strings.Join(parts, " ")
}

// String returns a string representation of the geoname
func (g *Geoname) String() string {
	return fmt.Sprintf("%d: %s (%s, %s)", g.ID, g.Name, g.CountryCode, g.FeatureCode)
}
