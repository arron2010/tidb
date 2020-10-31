package sql_test

import (
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testkit"
	"testing"
	"time"
)

func TestBootstrapSession(t *testing.T) {
	logutil.SetLevel("error")
	//NewMVCCLevelDB NewMemDB
	//mvccStore,_ := mocktikv.NewMVCCLevelDB("")
	mvccStore, _ := mocktikv.NewMVCCLevelDB("")
	store, _ := mockstore.NewMockTikvStore(
		mockstore.WithMVCCStore(mvccStore),
	)
	session.BootstrapSession(store)
	c := &check.C{}
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int,c3 int,c4 int,c5 int,c6 int,c7 int,c8 int,c9 int,c10 int,c11 int,c12 int,c13 int,c14 int,c15 int,c16 int,c17 int,c18 int,c19 int,c20 int)")
	tk.MustExec("insert into t values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)

}
func Benchmark_InsertData(b *testing.B) {
	logutil.SetLevel("error")

	//NewMVCCLevelDB
	//NewMemDB
	mvccStore, _ := mocktikv.NewMemDB("")

	store, _ := mockstore.NewMockTikvStore(
		mockstore.WithMVCCStore(mvccStore),
	)
	//s, _ := session.CreateSession(store)
	session.BootstrapSession(store)
	time.Sleep(1 * time.Second)
	c := &check.C{}
	tk := testkit.NewTestKit(c, store)

	//tk.MustExec("drop table if exists t")
	//tk.MustExec("create table t(c1 int  auto_increment, c2 varchar(255))")
	//tk.MustExec("create index index_t on t(c2)")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int,c3 int,c4 int,c5 int,c6 int,c7 int,c8 int,c9 int,c10 int,c11 int,c12 int,c13 int,c14 int,c15 int,c16 int,c17 int,c18 int,c19 int,c20 int)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec("insert into t values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		//session.BootstrapSession(store)
		//tk.MustExec("insert into t values (?)", "AAAAAA")
	}
	//r := tk.MustQuery(`select * from t`)
	//fmt.Println(len(r.Rows()))
	b.StopTimer()

	//
}
