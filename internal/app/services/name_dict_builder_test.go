package services // ← важно: тот же package, что и тестируемый код

import (
	"testing"
)

func TestIsCJKRune(t *testing.T) {
	tests := []struct {
		name     string
		input    rune
		expected bool
	}{
		{"Chinese char", '北', true},   // U+5317
		{"Japanese Kanji", '漢', true}, // U+6F22
		{"Korean Hanja", '韓', true},   // U+97D3
		{"Hiragana", 'あ', true},       // U+3042
		{"Katakana", 'ア', true},       // U+30A2
		{"Latin A", 'A', false},
		{"Cyrillic М", 'М', false},
		{"Arabic Alef", 'ا', false}, // не CJK, но арабский
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isCJKRune(tt.input)
			if got != tt.expected {
				t.Errorf("isCJKRune(%q U+%04X) = %v; want %v",
					tt.input, tt.input, got, tt.expected)
			}
		})
	}
}

func TestIsArabicRune(t *testing.T) {
	tests := []struct {
		name     string
		input    rune
		expected bool
	}{
		{"Arabic Alef", 'ا', true},     // U+0627
		{"Arabic Yeh", 'ي', true},      // U+064A
		{"Arabic Extended", 'ࢤ', true}, // U+08A4
		{"Latin A", 'A', false},
		{"CJK 北", '北', false}, // не арабский
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isArabicRune(tt.input)
			if got != tt.expected {
				t.Errorf("isArabicRune(%q U+%04X) = %v; want %v",
					tt.input, tt.input, got, tt.expected)
			}
		})
	}
}

func TestShouldInclude(t *testing.T) {
	// Тестируем метод с разными конфигурациями
	tests := []struct {
		name          string
		input         string
		excludeCJK    bool
		excludeArabic bool
		expected      bool
	}{
		{"Latin with CJK off", "Beijing", false, false, true},
		{"Chinese with CJK off", "北京", false, false, true},
		{"Chinese with CJK on", "北京", true, false, false}, // должен отфильтроваться
		{"Arabic with Arabic on", "القاهرة", false, true, false},
		{"Mixed Latin+CJK with CJK on", "北京Beijing", true, false, false},
		{"Empty string", "", true, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Создаём mock-конфиг
			builder := &NameDictBuilder{
				excludeCJK:    tt.excludeCJK,
				excludeArabic: tt.excludeArabic,
			}
			got := builder.shouldInclude(tt.input)
			if got != tt.expected {
				t.Errorf("shouldInclude(%q, CJK=%v, Arabic=%v) = %v; want %v",
					tt.input, tt.excludeCJK, tt.excludeArabic, got, tt.expected)
			}
		})
	}
}
