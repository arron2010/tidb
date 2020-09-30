package buntdb

import (
	"github.com/pingcap/tidb/btree"
	"reflect"
)

func isNil(i interface{}) bool {
	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	}
	return false
}

type MemdbIterator struct {
	dbi       *dbItem
	validated bool
	cursor    *btree.Cursor
}

func (m *MemdbIterator) Next() {
	item := m.cursor.Next()
	if item == nil || isNil(item) {
		m.dbi = nil
		m.validated = false
		return
	}
	m.dbi = item.(*dbItem)
}
func (m *MemdbIterator) Valid() bool {
	return m.validated
}

func (m *MemdbIterator) Key() []byte {
	if m.dbi != nil {
		return m.dbi.key
	} else {
		return nil
	}
}

func (m *MemdbIterator) Value() []byte {
	if m.dbi != nil {
		return m.dbi.val
	} else {
		return nil
	}
}

//	Key()
