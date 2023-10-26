package util

import (
	"fmt"
	"strings"
	"testing"
)

func TestToShortFileName(t *testing.T) {
	fmt.Println(strings.TrimPrefix("/buckets/common", "/buckets"))
	tests := []struct {
		in    string
		value string
	}{
		{"/data/a/b/c/d.txt", "/data/a/b/c/d.txt"},
		{"/data/a/b/c/очень_длинное_имя_файла_c_подробным_указанием_наименования_и_содержания_стандартизованных_форм_за_анварь_-_июнь_2023_года(РОГА_И_КОПЫТА_ООО).txt", "/data/a/b/c/очень_длинное_имя_файла_c_подробным_указанием_наименования_и_содержания_стандартизованных_форм_за_анварь_-_июнь_2023_года(РОГА_И_КОПЫТ354fcaf4.txt"},
		{"/data/a/b/c/очень_длинное_имя_файла_c_подробным_указанием_наименования_и_содержания_стандартизованных_форм_за_анварь_-_июнь_2023_года(РОГА_И_КОПЫТА_ООО)_without_extension", "/data/a/b/c/очень_длинное_имя_файла_c_подробным_указанием_наименования_и_содержания_стандартизованных_форм_за_анварь_-_июнь_2023_года(РОГА_И_КОПЫТА_О21a6e47a"},
	}
	for _, p := range tests {
		got := ToShortFileName(p.in)
		if got != p.value {
			t.Errorf("failed to test: got %v, want %v", got, p.value)
		}
	}
}
