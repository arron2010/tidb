package chunk

import (
	"fmt"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"testing"
	"unsafe"
)

var nullBitmap []byte

func appendNullBitmap(notNull bool, length int) {
	idx := length >> 3 //等同于取8的倍数
	if idx >= len(nullBitmap) {
		nullBitmap = append(nullBitmap, 0)
	}
	if notNull {
		pos := uint(length) & 7 //等同于取7的余数
		b := byte(1 << pos)
		nullBitmap[idx] = nullBitmap[idx] | b
	}
}
func TestAppendNullBitmap(t *testing.T) {
	nullBitmap = make([]byte, 0, 0)
	for i := 0; i < 9; i++ {
		appendNullBitmap(true, i)
	}
	fmt.Println(nullBitmap)
}
func TestChunk_Append(t *testing.T) {
	maxChunkSize := 1024
	sqlTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeLong),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeString),
	}
	x := 8 >> 3
	fmt.Println(x)
	chk := New(sqlTypes, maxChunkSize, maxChunkSize)
	chk.AppendUint64(0, 100)
	chk.AppendUint64(0, 101)
	chk.AppendUint64(0, 102)
	chk.AppendUint64(0, 103)
	chk.AppendUint64(0, 104)
	chk.AppendUint64(0, 105)
	chk.AppendUint64(0, 106)
	chk.AppendUint64(0, 107)

	chk.AppendFloat64(1, 0.05)
	chk.AppendString(2, "AAA")
	row := chk.GetRow(0)
	fmt.Println(row.GetUint64(0))

	fmt.Printf("RequiredRows:%d NumRows:%d\n", chk.RequiredRows(), chk.NumRows())
	//fmt.Println(chk.RequiredRows())
}

func TestPointer(t *testing.T) {
	var x *uint64
	var data uint64
	data = 10
	x = &data

	data2 := 20
	p := unsafe.Pointer(&x)
	p = unsafe.Pointer(&data2)
	z := (*uint64)(p)
	fmt.Println(*z)

}
