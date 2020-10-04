package mocktikv

import (
	"fmt"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/tablecodec"
	"strconv"
	"strings"
	"testing"
)

//var store2 *MVCCMemDB
//func  mustPutOKEx(key, value string, startTS, commitTS uint64) error{
//	req := &kvrpcpb.PrewriteRequest{
//		Mutations:    putMutations(key, value),
//		PrimaryLock:  []byte(key),
//		StartVersion: startTS,
//	}
//	errs := store2.Prewrite(req)
//	if len(errs) > 0 && errs[0] != nil{
//		return errs[0]
//	}
//	err := store2.Commit([][]byte{[]byte(key)}, startTS, commitTS)
//	return err
//}
//func  mustGetOKEx(key string, ts uint64)(string,error) {
//	val, err := store2.Get([]byte(key), ts, kvrpcpb.IsolationLevel_SI, nil)
//	return string(val),err
//}
func TestMVCCMemDB_Commit(t *testing.T) {
	var err error
	key := "X"
	value := "A"
	expect := ""
	store, err = NewMemDB("")
	for i := 1; i <= 100; i++ {
		key = "X" + strconv.Itoa(i)
		value = "A" + strconv.Itoa(i)
		err = mustPutOK(key, value, 5+uint64(i), 10+uint64(i))
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	key = "X100"
	expect, err = mustGetOK(key, 1000)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("expect value:", expect)
}

func mustRangeScanOK(start, end string, limit int, ts uint64) []Pair {
	pairs := store.Scan([]byte(start), []byte(end), limit, ts, kvrpcpb.IsolationLevel_SI, nil)
	return pairs
}
func TestMVCCMemDB_Scan(t *testing.T) {
	var err error
	store, err = NewMemDB("")
	if err != nil {
		panic(err)
	}
	mustPutOK("A", "A10", 5, 10)
	mustPutOK("C", "C10", 5, 10)
	mustPutOK("E", "E10", 5, 10)
	mustPutOK("B", "B20", 15, 20)
	mustPutOK("D", "D20", 15, 20)
	pairs := mustRangeScanOK("B", "D\x00", 5, 20)
	for _, item := range pairs {
		fmt.Println(string(item.Key))
	}
}

///

//###xp-> [116 128 0 0 0 0 0 0 47 95 105 128 0 0 0 0 0 0 1 1 49 0 0 0 0 0 0 0 248 3 128 0 0 0 0 0 0 100]
//###xp-> [116 128 0 0 0 0 0 0 47 95 114 128 0 0 0 0 0 0 100]

//116 128 0 0 0 0 0 0 47 95 114 128 0 0 0 0 0 0 100]
func convertBytes(str string) []byte {
	chars := strings.Split(str, " ")
	buf := make([]byte, 0, len(chars))
	for i := 0; i < len(chars); i++ {
		b, _ := strconv.ParseUint(chars[i], 10, 8)
		buf = append(buf, byte(b))
	}
	return buf
}
func TestDecodeIndexKey(t *testing.T) {
	str := `116 128 0 0 0 0 0 0 47 95 105 128 0 0 0 0 0 0 1 1 49 0 0 0 0 0 0 0 248 3 128 0 0 0 0 0 0 100`
	buf := convertBytes(str)

	tableID, indexID, indexValues, err := tablecodec.DecodeIndexKey(buf)
	fmt.Println(tableID, " | ", indexID, " | ", indexValues, " | ", err)
}
func TestDecodeRecordKey(t *testing.T) {
	str := `116 128 0 0 0 0 0 0 47 95 114 128 0 0 0 0 0 0 100`
	buf := convertBytes(str)

	prefix := buf[9:11]
	fmt.Println(string(prefix))
	tableID, handle, err := tablecodec.DecodeRecordKey(buf)
	fmt.Println(tableID, " ", handle, " ", err)
}
