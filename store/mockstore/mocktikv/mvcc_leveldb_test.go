package mocktikv

import (
	"fmt"
	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"testing"
)

//var mvcc *MVCCLevelDB
//func TestMain(m *testing.M) {
//	//var err error
//	//mvcc, err = NewMVCCLevelDB("/opt/tidbtest/test.db")
//	//if err != nil{
//	//	panic(err)
//	//}
//}
var dbPath = "/opt/tidbtest/testdb01"
var store MVCCStore

func mustPrewriteWithTTLOK(mutations []*kvrpcpb.Mutation, primary string, startTS uint64, ttl uint64) error {
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    mutations,
		PrimaryLock:  []byte(primary),
		StartVersion: startTS,
		LockTtl:      ttl,
		MinCommitTs:  startTS + 1,
	}
	errs := store.Prewrite(req)
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
func mustPrewriteOK(mutations []*kvrpcpb.Mutation, primary string, startTS uint64) {
	mustPrewriteWithTTLOK(mutations, primary, startTS, 0)
}

func mustPutOK(key, value string, startTS, commitTS uint64) error {
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations(key, value),
		PrimaryLock:  []byte(key),
		StartVersion: startTS,
	}
	errs := store.Prewrite(req)
	if len(errs) > 0 && errs[0] != nil {
		return errs[0]
	}
	err := store.Commit([][]byte{[]byte(key)}, startTS, commitTS)
	return err
}

func mustGetOK(key string, ts uint64) (string, error) {
	val, err := store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_SI, nil)
	return string(val), err
}

type test01 struct {
}

func (t *test01) TestCommitConflict() {

}

//测试提交冲突
func TestCommitConflict(t *testing.T) {
	c := check.NewC("TestCommitConflict", &test01{})
	s := &testMockTiKVSuite{}
	s.store, _ = NewMVCCLevelDB("")

	s.mustPrewriteOK(c, putMutations("x", "A"), "x", 5)

	//s.mustGetOK(c, "x", 3, "A")

	req := &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations("x", "B"),
		PrimaryLock:  []byte("x"),
		StartVersion: 5,
	}
	errs := s.store.Prewrite(req)
	if errs[0] != nil {
		panic(errs[0])
	}
	//	s.mustRollbackOK(c, [][]byte{[]byte("x")}, 5)

	//err := s.store.Commit([][]byte{[]byte("x")}, 10, 20)
	//if err != nil{
	//	panic(err)
	//}

	//s.TestCommitConflict(c)
	//m := &test01{}
	//h := reflect.ValueOf(m)
	//fmt.Println(h.Method(0))
	//fmt.Println(h.Type().Method(0))
	//var err error
	//err = errors.New("aa")
	//c.Assert(err,check.IsNil)
	//mustPrewriteOK(putMutations("x", "A"), "x", 5)

}

func TestMVCCLevelDB_Commit(t *testing.T) {
	var err error
	store, err = NewMVCCLevelDB("")
	expect := ""
	key := ""
	value := ""
	var i uint64
	//startTS := uint64(5)
	//commitTS :=uint64(10)10
	for i = 1; i <= 1; i++ {
		key = "x"   //"key_" + strconv.Itoa(int(i))
		value = "A" //"value_"+strconv.Itoa(int(i))
		err = mustPutOK(key, value, 5+i, 10+i)
	}
	if err != nil {
		t.Fatal(err)
	}
	//startTS = 10
	key = "x"
	expect, err = mustGetOK(key, 21)
	fmt.Println("expect value:", expect)
}
