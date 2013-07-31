package kdb

import (
	"bytes"
	"fmt"
	"github.com/sdming/kdb/ansi"
)

const _indentChar = "\t"

type sqlWriter struct {
	depth int
	bytes.Buffer
}

func (sw *sqlWriter) Blank() {
	sw.WriteString(ansi.Blank)
}

func (sw *sqlWriter) IndentOuter() {
	if sw.depth > 0 {
		sw.depth--
	}
}

func (sw *sqlWriter) IndentInner() {
	sw.depth++
}

func (sw *sqlWriter) Comma() {
	sw.WriteString(ansi.Comma)
	sw.WriteString(" ")
}

func (sw *sqlWriter) LineBreak() {
	sw.WriteString(ansi.LineBreak)
	for i := 0; i < sw.depth; i++ {
		sw.WriteString(_indentChar)
	}
}

func (sw *sqlWriter) OpenParentheses() {
	sw.WriteString(ansi.OpenParentheses)
}

func (sw *sqlWriter) CloseParentheses() {
	sw.WriteString(ansi.CloseParentheses)
}

func (sw *sqlWriter) Print(args ...string) {
	for i := 0; i < len(args); i++ {
		sw.WriteString(args[i])
	}
}

func (sw *sqlWriter) PrintSplit(split string, args ...string) {
	for i := 0; i < len(args); i++ {
		if i > 0 {
			sw.WriteString(split)
		}
		sw.WriteString(args[i])
	}
}

func (sw *sqlWriter) Println(args ...interface{}) {
	sw.WriteString(fmt.Sprintln(args...))
}
