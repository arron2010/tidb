package xhelper

import (
	"fmt"
	"github.com/pingcap/tidb/util/codec"
	"strconv"
	"strings"
)

func DecodeListDataKey(key []byte) (prefix string, content string, index int64, dt uint64) {
	prefix = string(key[0])
	content = ""
	index = -1
	dt = 0
	l := len(key)
	buf := key
	l = len(buf)
	if l < 16 {
		return prefix, content, index, dt
	}
	_, index, _ = codec.DecodeInt(buf[l-8 : l])
	_, dt, _ = codec.DecodeUint(buf[l-16 : l-8])
	_, b, _ := codec.DecodeBytes(buf[1:], nil)
	if len(b) < 2 {
		return prefix, content, index, dt
	}
	content = string(b)
	return prefix, content, index, dt
}

func Print(op string, args ...interface{}) {
	if len(args) > 0 && args[0] == "USE mysql" {
		fmt.Println()
	}
	fmt.Print(op)
	fmt.Println(args...)
}

var seq uint64

func convertBytes(str string) []byte {
	chars := strings.Split(str, " ")
	buf := make([]byte, 0, len(chars))
	for i := 0; i < len(chars); i++ {
		b, _ := strconv.ParseUint(chars[i], 10, 8)
		buf = append(buf, byte(b))
	}
	return buf
}

var c1 int
var c2 int

func PrintKey2(op string, key []byte) {

	//key2 := convertBytes(`109 68 68 76 74 111 98 65 100 255 100 73 100 120 76 105 115 116 255 0 0 0 0 0 0 0 0 247 0 0 0 0 0 0 0 76`)
	//if key[0]==109 && key[len(key)-1]==76{
	//	//debug.PrintStack()
	//	c1 = c1 +1
	//	fmt.Println(op,key,"------->",c1)
	//}
}
func PrintKey3(op string, key []byte, val []byte) {

	//key2 := convertBytes(`109 68 68 76 74 111 98 65 100 255 100 73 100 120 76 105 115 116 255 0 0 0 0 0 0 0 0 247 0 0 0 0 0 0 0 76`)
	if key[0] == 109 && key[len(key)-1] == 76 {

		fmt.Println(op, "--->", key, "-->", val, "-->", len(val))
	}
}

func PrintKey(op string, key []byte) {
	prefix := string(key[0])
	if prefix == "m" {
		//_ ,content ,index ,dt  := DecodeListDataKey(key)
		p, content, index, dt := DecodeListDataKey(key)
		if strings.Index(op, "MVCCLevelDB_Scan") > -1 {
			fmt.Println(op, "prefix-->", p, " content-->", content, " index-->", index, " dt-->", dt, " raw-->", key)
		}

		//if dt != 108{ //ListData
		//	return
		//}

		//if strings.Index(op,"Commit") > -1{
		//	if content == "DDLJobList"{
		//		seq = seq +1
		//		fmt.Println(op," : ",content, "index--->",index)
		//	}
		//}

		//if strings.Index(op,"Commit") > -1{
		//	fmt.Println(op," : ",string(key))
		//}

		if strings.Index(op, "memDbBuffer_Set") > -1 {
			//_ ,content ,index ,_  := DecodeListDataKey(key)
			if content == "DDLJobList" {
				seq = seq + 1
				//fmt.Println(op," : ",content, "index--->",index)
			}
		}

	}
}
