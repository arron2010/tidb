package mocktikv

import (
	"github.com/pingcap/check"
	"testing"
)

type testScan struct {
}

func (t *testScan) TestScan() {

}
func TestScan(t *testing.T) {
	c := check.NewC("TestScan", &testScan{})
	s := &testMockTiKVSuite{}
	s.store, _ = NewMemDB("")
	//s.store,_=  NewMVCCLevelDB("")
	s.TestScan(c)
}

type testBatchGet struct {
}

func (t *testBatchGet) TestBatchGet() {

}

func TestBatchGet(t *testing.T) {
	c := check.NewC("TestBatchGet", &testBatchGet{})
	s := &testMockTiKVSuite{}
	s.store, _ = NewMemDB("")
	s.TestBatchGet(c)
}

type testCheckTxnStatus struct {
}

func (t *testCheckTxnStatus) TestCheckTxnStatus() {

}

func TestCheckTxnStatus(t *testing.T) {
	c := check.NewC("TestBatchGet", &testCheckTxnStatus{})
	s := &testMockTiKVSuite{}
	s.store, _ = NewMemDB("")
	db := &testMVCCLevelDB{}
	db.testMockTiKVSuite = *s
	db.TestCheckTxnStatus(c)
}

type testReverseScan struct {
}

func (t *testReverseScan) TestReverseScan() {

}

func TestReverseScan(t *testing.T) {
	c := check.NewC("TestReverseScan", &testReverseScan{})
	s := &testMockTiKVSuite{}
	s.store, _ = NewMemDB("")
	//s.store,_=  NewMVCCLevelDB("")
	s.TestReverseScan(c)
}
