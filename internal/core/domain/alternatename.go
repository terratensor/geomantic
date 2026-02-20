package domain

type AlternateName struct {
	ID              int64
	GeonameID       int64
	ISOLanguage     string
	AlternateName   string
	IsPreferredName bool
	IsShortName     bool
	IsColloquial    bool
	IsHistoric      bool
	From            *string
	To              *string
}

func (a *AlternateName) IsRussian() bool {
	return a.ISOLanguage == "ru"
}

func (a *AlternateName) IsEnglish() bool {
	return a.ISOLanguage == "en"
}

func (a *AlternateName) LanguageGroup() string {
	switch a.ISOLanguage {
	case "ru":
		return "ru"
	case "en":
		return "en"
	default:
		return "other"
	}
}
