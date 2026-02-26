package services

import (
	"log"
	"testing"
)

func TestKoreanWords(t *testing.T) {
	koreanWords := []string{
		"한국폴리텍2대학인천캠퍼스",
		"담양2터널",
		"타워팰리스4차",
		"검단힐스테이트5차아파트",
		"다목2리",
	}

	log.Println("=== Проверка корейских слов ===")
	for _, word := range koreanWords {
		log.Printf("\nСлово: %q", word)
		for i, r := range word {
			isCJK := isCJKRune(r)
			log.Printf("  [%d] '%c' (U+%04X) -> isCJK: %v", i, r, r, isCJK)
		}
		// Проверка через shouldInclude
		builder := &NameDictBuilder{excludeCJK: true}
		log.Printf("  shouldInclude (CJK=true): %v", builder.shouldInclude(word))
	}
}

func TestIsCJKRune_Comprehensive(t *testing.T) {
	tests := []struct {
		name  string
		input rune
		want  bool
	}{
		// Китайские иероглифы
		{"Chinese 北", '北', true}, // U+5317
		{"Chinese 京", '京', true}, // U+4EAC

		// Японские kana
		{"Hiragana あ", 'あ', true}, // U+3042
		{"Katakana ア", 'ア', true}, // U+30A2

		// Корейский Hangul
		{"Hangul 가", '가', true}, // U+AC00
		{"Hangul 한", '한', true}, // U+D55C
		{"Hangul 컴", '컴', true}, // U+CEF4

		// Латиница/кириллица — должны быть false
		{"Latin A", 'A', false},
		{"Cyrillic М", 'М', false},
		{"Digit 2", '2', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCJKRune(tt.input)
			if got != tt.want {
				t.Errorf("isCJKRune(%q U+%04X) = %v; want %v",
					tt.input, tt.input, got, tt.want)
			}
		})
	}
}
