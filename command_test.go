package kdb

import (
	"testing"
)

func TestCompile(t *testing.T) {
	texts := []string{
		"{a}{b}{c}",
		"{ a } { b } { c }",
		"{ a } b }{ c } ",
		"{a}",
		"a,b,c",
		"a,{b},c",
	}

	for _, text := range texts {
		s, names, err := CompileTemplate(text)
		if err != nil {
			t.Error("compile error", err, text)
		}
		t.Log(s, names)
	}

	texts = []string{
		"{",
		"{a}{",
	}

	for _, text := range texts {
		_, _, err := CompileTemplate(text)
		if err == nil {
			t.Error("compile should return error", text)
		}
	}

}
