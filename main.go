package main

import (
	"bytes"
	"fmt"
)

func main() {

	items := create(
		"items",
		[]string{"item_id", "item_name", "type_id", "price"},
	)
	items.insert(1, "apple", 1, 300)
	items.insert(2, "orange", 1, 130)
	items.insert(3, "cabbage", 2, 200)
	items.insert(4, "seaweed", nil, 250)
	items.insert(5, "mushroom", 3, 180)

	types := create(
		"types",
		[]string{"type_id", "type_name"},
	)
	types.insert(1, "fruit")
	types.insert(2, "vegetable")

	fmt.Println(items)
	fmt.Println(from("items"))
	fmt.Println(from("items").selectQ("item_name", "price"))
	fmt.Println(from("items").lessThan("price", 250))
	fmt.Println(from("items").leftJoin("types", "type_id"))
}

var tables = map[string]*table{}

type column struct {
	parent string
	name   string
}

func newColumn(parent, name string) *column {
	return &column{parent: parent, name: name}
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
	// we can simplify checking the existence of n in r,
	// by r.findColumn(n) <= len(r.columns) before random accesses
	return len(r.columns)
}

type table struct {
	relation
	name string
}

func newTable(name string, cols []*column) *table {
	t := &table{}
	t.name = name
	t.columns = cols
	t.tuples = []*tuple{}
	return t
}

func create(name string, colNames []string) *table {
	cols := []*column{}
	for _, cn := range colNames {
		cols = append(cols, newColumn("", cn))
	}
	t := newTable(name, cols)
	tables[name] = t
	return t
}

func (t *table) insert(vals ...interface{}) *table {
	t.tuples = append(t.tuples, newTuple(vals))
	return t
}

type query struct {
	relation
}

func newQuery(cols []*column, tups []*tuple) *query {
	q := &query{}
	q.columns = cols
	q.tuples = tups
	return q
}

func from(tableName string) *query {
	t := tables[tableName]
	newCols := []*column{}
	for _, c := range t.columns {
		newCols = append(newCols, newColumn(t.name, c.name))
	}
	return newQuery(newCols, t.tuples)
}

func (q *query) selectQ(colNames ...string) *query {
	idxs := []int{}
	newCols := []*column{}
	for _, cn := range colNames {
		idx := q.findColumn(cn)
		idxs = append(idxs, idx)
		if idx < len(q.columns) {
			newCols = append(newCols, q.columns[idx])
		}
	}
	newTups := []*tuple{}
	for _, tup := range q.tuples {
		vals := []interface{}{}
		for _, idx := range idxs {
			// TODO: Can I avoid to refer the nil pointer?
			if idx < len(tup.values) {
				vals = append(vals, tup.values[idx])
			} else {
				vals = append(vals, nil)
			}
		}
		newTups = append(newTups, newTuple(vals))
	}
	return newQuery(newCols, newTups)
}

func (q *query) leftJoin(tableName, colName string) *query {
	t := tables[tableName]
	newCols := []*column{}
	newCols = append(newCols, q.columns...)
	for _, c := range t.columns {
		newCols = append(newCols, newColumn(tableName, c.name))
	}
	lIdx, rIdx := q.findColumn(colName), t.findColumn(colName)
	if len(q.columns) <= lIdx || len(t.columns) <= rIdx {
		return newQuery(newCols, []*tuple{})
	}
	newTups := []*tuple{}
	for _, lTup := range q.tuples {
		if len(lTup.values) <= lIdx {
			continue
		}
		keyVal := lTup.values[lIdx]
		vals := []interface{}{}
		vals = append(vals, lTup.values...)
		// join at non-nil values only
		if keyVal != nil {
			for _, rTup := range t.tuples {
				if len(rTup.values) <= rIdx {
					continue
				}
				if rTup.values[rIdx] == keyVal {
					vals = append(vals, rTup.values...)
					break // join at most one tuple from the rightside table
				}
			}
		}
		for len(vals) < len(newCols) {
			vals = append(vals, nil)
		}
		newTups = append(newTups, newTuple(vals))
	}
	return newQuery(newCols, newTups)
}

func (q *query) lessThan(colName string, n int) *query {
	idx := q.findColumn(colName)
	if idx >= len(q.columns) {
		return newQuery(q.columns, []*tuple{})
	}
	newTups := []*tuple{}
	for _, tup := range q.tuples {
		v, ok := tup.values[idx].(int)
		if ok && v < n {
			newTups = append(newTups, tup)
		}
	}
	return newQuery(q.columns, newTups)
}
