package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFindColumnFound(t *testing.T) {
	cols := []*column{
		newColumn("", "zero"),
		newColumn("", "one"),
		newColumn("", "two"),
	}
	r := &relation{columns: cols, tuples: nil}
	cases := []struct {
		in  string
		out int
	}{
		{"zero", 0}, {"one", 1}, {"two", 2},
	}
	for _, c := range cases {
		assert.Equal(t, c.out, r.findColumn(c.in))
	}
}

func TestFindColumnNotFound(t *testing.T) {
	cols := []*column{
		newColumn("", "foo"),
	}
	r := &relation{columns: cols, tuples: nil}
	assert.Equal(
		t, 1, r.findColumn("other_name"),
		"it should return the length of columns",
	)
}

func TestCreateRegistered(t *testing.T) {
	create("TestCreateRegistered", []string{"col_name"})
	tbl := tables["TestCreateRegistered"]
	if assert.NotNil(t, tbl) {
		assert.Equal(t, "TestCreateRegistered", tbl.name)
		assert.Equal(t, []*column{newColumn("", "col_name")}, tbl.columns)
		assert.Equal(t, []*tuple{}, tbl.tuples)
	}
}

func TestCreateNotRegistered(t *testing.T) {
	create("TestCreateNotRegistered", []string{"col_name"})
	tbl := tables["other_name"]
	assert.Nil(t, tbl)
}

func TestInsertTrivial(t *testing.T) {
	tbl := &table{name: "tbl_name"}
	newTbl := tbl.insert()
	assert.Equal(t, tbl, newTbl)
}

func TestInsertOrdered(t *testing.T) {
	tbl := &table{name: "tbl_name"}
	tbl.columns = []*column{newColumn("", "id")}
	tbl.insert(0).insert(1).insert(2)
	assert.Equal(t, 0, tbl.tuples[0].values[0])
	assert.Equal(t, 1, tbl.tuples[1].values[0])
	assert.Equal(t, 2, tbl.tuples[2].values[0])
}

func TestFromEmpty(t *testing.T) {
	tbl := create("TestFromEmpty", []string{"id"})
	r := from("TestFromEmpty")
	assert.Equal(t, 1, len(r.columns))
	assert.Equal(t, newColumn("TestFromEmpty", "id"), r.columns[0])
	assert.Equal(t, tbl.columns[0].name, r.columns[0].name)
	assert.Equal(t, 0, len(r.tuples))
}

func TestFromAfterInsert(t *testing.T) {
	tbl := create("TestFromAfterInsert", []string{"id"})
	tbl.insert(0).insert(1).insert(2)
	r := from("TestFromAfterInsert")
	assert.Equal(t, 1, len(r.columns))
	assert.Equal(t, newColumn("TestFromAfterInsert", "id"), r.columns[0])
	assert.Equal(t, tbl.columns[0].name, r.columns[0].name)
	assert.Equal(t, tbl.tuples, r.tuples)
}

func TestFromByRelation(t *testing.T) {
	src := &relation{
		columns: []*column{newColumn("TestFromByRelation", "id")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0}},
			&tuple{values: []interface{}{1}},
			&tuple{values: []interface{}{2}},
		},
	}
	r := from(src)
	assert.Equal(t, src.columns, r.columns)
	assert.Equal(t, src.tuples, r.tuples)
}

func TestSelectQNone(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "str")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
			&tuple{values: []interface{}{1, "one"}},
		},
	}
	res := r.selectQ()
	assert.Equal(t, 0, len(res.columns))
	assert.Equal(t, 2, len(res.tuples))
}

func TestSelectQUnknown(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "str")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
			&tuple{values: []interface{}{1, "one"}},
		},
	}
	res := r.selectQ("unknown")
	assert.Equal(t, 0, len(res.columns))
	assert.Equal(t, 2, len(res.tuples))
}

func TestSelectQProper(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "str")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
			&tuple{values: []interface{}{1, "one"}},
		},
	}
	res := r.selectQ("str")
	assert.Equal(t, []*column{newColumn("", "str")}, res.columns)
	assert.Equal(t, "zero", res.tuples[0].values[0], "zero")
	assert.Equal(t, "one", res.tuples[1].values[0], "one")
}

func TestSelectQAll(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "str")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
			&tuple{values: []interface{}{1, "one"}},
		},
	}
	res := r.selectQ("id", "str")
	assert.Equal(t, r, res)
}

func TestLessThanUnknown(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0}},
			&tuple{values: []interface{}{1}},
		},
	}
	res := r.lessThan("unknown", 0)
	assert.Equal(t, r.columns, res.columns)
	assert.Equal(t, 0, len(res.tuples))
}

func TestLessThanNone(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0}},
			&tuple{values: []interface{}{1}},
		},
	}
	res := r.lessThan("id", 0)
	assert.Equal(t, r.columns, res.columns)
	assert.Equal(t, 0, len(res.tuples))
}

func TestLessThanProper(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0}},
			&tuple{values: []interface{}{1}},
		},
	}
	res := r.lessThan("id", 1)
	assert.Equal(t, r.columns, res.columns)
	assert.Equal(t, 1, len(res.tuples))
	assert.Equal(t, []interface{}{0}, res.tuples[0].values)
}

func TestEqualUnknown(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{"zero"}},
			&tuple{values: []interface{}{"one"}},
		},
	}
	res := r.equal("unknown", "foo")
	assert.Equal(t, r.columns, res.columns)
	assert.Equal(t, 0, len(res.tuples))
}

func TesTEqualNone(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{"zero"}},
			&tuple{values: []interface{}{"one"}},
		},
	}
	res := r.equal("name", "two")
	assert.Equal(t, r.columns, res.columns)
	assert.Equal(t, 0, len(res.tuples))
}

func TestEqualTypeMismatch(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{"zero"}},
			&tuple{values: []interface{}{"one"}},
		},
	}
	res := r.equal("name", 0)
	assert.Equal(t, r.columns, res.columns)
	assert.Equal(t, 0, len(res.tuples))
}

func TestEqualProper(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{"zero"}},
			&tuple{values: []interface{}{"one"}},
		},
	}
	res := r.equal("name", "zero")
	assert.Equal(t, r.columns, res.columns)
	assert.Equal(t, 1, len(res.tuples))
	assert.Equal(t, []interface{}{"zero"}, res.tuples[0].values)
}

func TestOrderByUnknown(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
			&tuple{values: []interface{}{1, "one"}},
			&tuple{values: []interface{}{2, "two"}},
		},
	}
	res := r.orderBy("unknown")
	assert.Equal(t, r, res)
}

func TestOrderByNone(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples:  []*tuple{},
	}
	res := r.orderBy("id")
	assert.Equal(t, 0, len(res.tuples))
}

func TestOrderByAlreadySortedByInt(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
			&tuple{values: []interface{}{1, "one"}},
			&tuple{values: []interface{}{2, "two"}},
		},
	}
	res := r.orderBy("id")
	assert.Equal(t, 3, len(res.tuples))
	assert.Equal(t, []interface{}{0, "zero"}, res.tuples[0].values)
	assert.Equal(t, []interface{}{1, "one"}, res.tuples[1].values)
	assert.Equal(t, []interface{}{2, "two"}, res.tuples[2].values)
}

func TestOrderByAlreadySortedByString(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{1, "one"}},
			&tuple{values: []interface{}{2, "two"}},
			&tuple{values: []interface{}{0, "zero"}},
		},
	}
	res := r.orderBy("name")
	assert.Equal(t, 3, len(res.tuples))
	assert.Equal(t, []interface{}{1, "one"}, res.tuples[0].values)
	assert.Equal(t, []interface{}{2, "two"}, res.tuples[1].values)
	assert.Equal(t, []interface{}{0, "zero"}, res.tuples[2].values)
}

func TestOrderByProperByInt(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{1, "one"}},
			&tuple{values: []interface{}{2, "two"}},
			&tuple{values: []interface{}{0, "zero"}},
		},
	}
	res := r.orderBy("id")
	assert.Equal(t, 3, len(res.tuples))
	assert.Equal(t, []interface{}{0, "zero"}, res.tuples[0].values)
	assert.Equal(t, []interface{}{1, "one"}, res.tuples[1].values)
	assert.Equal(t, []interface{}{2, "two"}, res.tuples[2].values)
}

func TestOrderByProperByString(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
			&tuple{values: []interface{}{1, "one"}},
			&tuple{values: []interface{}{2, "two"}},
		},
	}
	res := r.orderBy("name")
	assert.Equal(t, 3, len(res.tuples))
	assert.Equal(t, []interface{}{1, "one"}, res.tuples[0].values)
	assert.Equal(t, []interface{}{2, "two"}, res.tuples[1].values)
	assert.Equal(t, []interface{}{0, "zero"}, res.tuples[2].values)
}

func TestLeftJoinLeftUnknown(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
		},
	}
	tbl := create("TestLeftJoinLeftUnknown", []string{"id", "size"})
	tbl.insert(0, 100)
	res := r.leftJoin("TestLeftJoinLeftUnknown", "size")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 0, len(res.tuples))
}

func TestLeftJoinLeftUnknownByRelation(t *testing.T) {
	r1 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
		},
	}
	r2 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "size")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, 100}},
		},
	}
	res := r1.leftJoin(r2, "size")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 0, len(res.tuples))
}

func TestLeftJoinRightUnknown(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
		},
	}
	tbl := create("TestLeftJoinRightUnknown", []string{"id", "size"})
	tbl.insert(0, 100)
	res := r.leftJoin("TestLeftJoinLeftUnknown", "name")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 1, len(res.tuples))
	assert.Equal(t, []interface{}{0, "zero", nil, nil}, res.tuples[0].values)
}

func TestLeftJoinRightUnknownByRelation(t *testing.T) {
	r1 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
		},
	}
	r2 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "size")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, 100}},
		},
	}
	res := r1.leftJoin(r2, "name")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 1, len(res.tuples))
	assert.Equal(t, []interface{}{0, "zero", nil, nil}, res.tuples[0].values)
}

func TestLeftJoinProper(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples:  []*tuple{&tuple{values: []interface{}{0, "zero"}}},
	}
	tbl := create("TestLeftJoinProper", []string{"id", "size"})
	tbl.insert(0, 100)
	res := r.leftJoin("TestLeftJoinProper", "id")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 1, len(res.tuples))
	assert.Equal(t, []interface{}{0, "zero", 0, 100}, res.tuples[0].values)
}

func TestLeftJoinProperByRelation(t *testing.T) {
	r1 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
		},
	}
	r2 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "size")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, 100}},
		},
	}
	res := r1.leftJoin(r2, "id")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 1, len(res.tuples))
	assert.Equal(t, []interface{}{0, "zero", 0, 100}, res.tuples[0].values)
}

func TestLeftJoinNotFound(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
		},
	}
	tbl := create("TestLeftJoinNotFound", []string{"id", "size"})
	tbl.insert(1, 100)
	res := r.leftJoin("TestLeftJoinNotFound", "id")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 1, len(res.tuples))
	assert.Equal(t, []interface{}{0, "zero", nil, nil}, res.tuples[0].values)
}

func TestLeftJoinNotFoundByRelation(t *testing.T) {
	r1 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
		},
	}
	r2 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "size")},
		tuples: []*tuple{
			&tuple{values: []interface{}{1, 100}},
		},
	}
	res := r1.leftJoin(r2, "id")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 1, len(res.tuples))
	assert.Equal(t, []interface{}{0, "zero", nil, nil}, res.tuples[0].values)
}

func TestLeftJoinNil(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{nil, "zero"}},
		},
	}
	tbl := create("TestLeftJoinNotFound", []string{"id", "size"})
	tbl.insert(nil, 100)
	res := r.leftJoin("TestLeftJoinNotFound", "id")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 1, len(res.tuples))
	assert.Equal(t, []interface{}{nil, "zero", nil, nil}, res.tuples[0].values)
}

func TestLeftJoinNilByRelation(t *testing.T) {
	r1 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{nil, "zero"}},
		},
	}
	r2 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "size")},
		tuples: []*tuple{
			&tuple{values: []interface{}{nil, 100}},
		},
	}
	res := r1.leftJoin(r2, "id")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 1, len(res.tuples))
	assert.Equal(t, []interface{}{nil, "zero", nil, nil}, res.tuples[0].values)
}

func TestLeftJoinMultiple(t *testing.T) {
	r := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
		},
	}
	tbl := create("TestLeftJoinMultiple", []string{"id", "size"})
	tbl.insert(0, 100)
	tbl.insert(0, 200)
	res := r.leftJoin("TestLeftJoinMultiple", "id")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 2, len(res.tuples))
	assert.Equal(t, []interface{}{0, "zero", 0, 100}, res.tuples[0].values)
	assert.Equal(t, []interface{}{0, "zero", 0, 200}, res.tuples[1].values)
}

func TestLeftJoinMultipleByRelation(t *testing.T) {
	r1 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "name")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, "zero"}},
		},
	}
	r2 := &relation{
		columns: []*column{newColumn("", "id"), newColumn("", "size")},
		tuples: []*tuple{
			&tuple{values: []interface{}{0, 100}},
			&tuple{values: []interface{}{0, 200}},
		},
	}
	res := r1.leftJoin(r2, "id")
	assert.Equal(t, 4, len(res.columns))
	assert.Equal(t, 2, len(res.tuples))
	assert.Equal(t, []interface{}{0, "zero", 0, 100}, res.tuples[0].values)
	assert.Equal(t, []interface{}{0, "zero", 0, 200}, res.tuples[1].values)
}
