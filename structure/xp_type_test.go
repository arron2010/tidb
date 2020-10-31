package structure

import (
	"fmt"
	xhelper "github.com/pingcap/tidb/helper"
	"github.com/pingcap/tidb/util/codec"
	"testing"
)

func TestTxStructure_EncodeListDataKey(t *testing.T) {
	s := &TxStructure{}
	str1 := "DDLJobList"
	s.prefix = []byte("m")
	b1 := s.encodeListDataKey([]byte(str1), 1)
	prefix, content, index, dt := xhelper.DecodeListDataKey(b1)
	fmt.Println("prefix:", prefix, " ", "content:", content, " ", "index:", index, " ", "dt:", dt)
}

func TestTxStructure_EncodeListDataKey2(t *testing.T) {
	str1 := "DDLJobList"
	b := []byte(str1)
	p := []byte("m")
	b1 := codec.EncodeBytes(p, b)
	_, b2, _ := codec.DecodeBytes(b1[1:], nil)
	fmt.Println(string(b2))
}
