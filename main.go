package main

import (
	"bytes"
	"fmt"
)

type column struct {
	parent string
	name   string
}

func newColumn(name string) *column {
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

type table struct {
	relation
	name string
}

func newTable(name string, cols []*column) *table {
	var t *table
	t.name = name
	t.columns = cols
	t.tuples = []*tuple{}
	return t
}

func create(name string, colNames []string) *table {
	cols := []*column{}
	for _, cn := range colNames {
		cols = append(cols, newColumn(cn))
	}
	// FIXME: register table to a global dictionary
	return newTable(name, cols)
}

func (t *table) insert(vals []interface{}) *table {
	t.tuples = append(t.tuples, newTuple(vals))
	return t
}

func main() {

}
