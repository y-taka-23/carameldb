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
	items.insert(4, "saury", 3, 220)
	items.insert(5, "seaweed", nil, 250)
	items.insert(6, "mushroom", 3, 180)

	types := create(
		"types",
		[]string{"type_id", "type_name"},
	)
	types.insert(1, "fruit")
	types.insert(2, "vegetable")
	types.insert(3, "fish")

	fmt.Println(items)
	var tblName tableName = "items"
	fmt.Println(from(tblName))
	/*
		fmt.Println(from("items").selectQ("item_name", "price"))
			fmt.Println(from("items").lessThan("price", 250))
			fmt.Println(from("items").leftJoin("types", "type_id"))
			fmt.Println(
				from(
					from("items").lessThan("price", 250),
				).leftJoin(
					from("types").lessThan("type_id", 3), "type_id",
				),
			)
	*/
}

type tableName string

func (tn tableName) String() string {
	return string(tn)
}

var tables = map[tableName]*table{}

type column struct {
	parent tableName
	name   string
}

func newColumn(parent tableName, name string) *column {
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

type relationer interface {
	relation() *relation
}

func (tblName tableName) relation() *relation {
	t := tables[tblName]
	cols := []*column{}
	for _, c := range t.columns {
		cols = append(cols, newColumn(tblName, c.name))
	}
	return newRelation(cols, t.tuples)
}

func (r *relation) relation() *relation {
	return r
}

func from(rel relationer) *relation {
	return rel.relation()
}

func newRelation(cols []*column, tups []*tuple) *relation {
	return &relation{columns: cols, tuples: tups}
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

func (r *relation) selectQ(colNames ...string) *relation {
	idxs := []int{}
	newCols := []*column{}
	for _, cn := range colNames {
		idx := r.findColumn(cn)
		idxs = append(idxs, idx)
		if idx < len(r.columns) {
			newCols = append(newCols, r.columns[idx])
		}
	}
	newTups := []*tuple{}
	for _, tup := range r.tuples {
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
	return newRelation(newCols, newTups)
}

func (r *relation) leftJoin(tblName tableName, colName string) *relation {
	t := tables[tblName]
	newCols := []*column{}
	newCols = append(newCols, r.columns...)
	for _, c := range t.columns {
		newCols = append(newCols, newColumn(tblName, c.name))
	}
	rIdx, tIdx := r.findColumn(colName), t.findColumn(colName)
	if len(r.columns) <= rIdx || len(t.columns) <= tIdx {
		return newRelation(newCols, []*tuple{})
	}
	newTups := []*tuple{}
	for _, rTup := range r.tuples {
		if len(rTup.values) <= rIdx {
			continue
		}
		keyVal := rTup.values[rIdx]
		vals := []interface{}{}
		vals = append(vals, rTup.values...)
		// join at non-nil values only
		if keyVal != nil {
			for _, tTup := range t.tuples {
				if len(tTup.values) <= tIdx {
					continue
				}
				if tTup.values[tIdx] == keyVal {
					vals = append(vals, tTup.values...)
					break // join at most one tuple from the rightside table
				}
			}
		}
		for len(vals) < len(newCols) {
			vals = append(vals, nil)
		}
		newTups = append(newTups, newTuple(vals))
	}
	return newRelation(newCols, newTups)
}

func (r *relation) lessThan(colName string, n int) *relation {
	idx := r.findColumn(colName)
	if idx >= len(r.columns) {
		return newRelation(r.columns, []*tuple{})
	}
	newTups := []*tuple{}
	for _, tup := range r.tuples {
		v, ok := tup.values[idx].(int)
		if ok && v < n {
			newTups = append(newTups, tup)
		}
	}
	return newRelation(r.columns, newTups)
}

func (r *relation) String() string {
	var buf bytes.Buffer
	for _, c := range r.columns {
		buf.WriteByte('|')
		if c.parent != "" {
			buf.WriteString(c.parent.String())
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

type table struct {
	relation
	name tableName
}

func newTable(name tableName, cols []*column) *table {
	t := &table{}
	t.name = name
	t.columns = cols
	t.tuples = []*tuple{}
	return t
}

func create(name tableName, colNames []string) *table {
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
