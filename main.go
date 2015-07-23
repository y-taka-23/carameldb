package main

import (
	"bytes"
	"fmt"
)

type column struct {
	parent string
	name   string
}

func newcolumn(name string) *column {
	return &column{parent: "", name: name}
}

type tuple struct {
	values []interface{}
}

func newTuple(vals []interface{}) *tuple {
	return &tuple{values: vals}
}

type relation struct {
	columns []*column
	tuples  []*tuple
}

func (r *relation) String() string {
	var buf bytes.Buffer
	for _, c := range r.columns {
		buf.WriteByte('|')
		if c.parent != "" {
			buf.WriteString(c.parent)
			buf.WriteByte('.')
		}
		buf.WriteString(c.name)
	}
	buf.WriteString("|\n")
	for _, t := range r.tuples {
		for _, v := range t.values {
			buf.WriteByte('|')
			buf.WriteString(fmt.Sprint(v))
		}
		buf.WriteString("|\n")
	}
	return buf.String()
}

func (r *relation) findColumn(name string) int {
	for i, c := range r.columns {
		if c.name == name {
			return i
		}
	}
	return -1
}

func main() {

}
