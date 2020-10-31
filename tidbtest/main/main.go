package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"os"
	"runtime"
	"time"
)

var store = flag.String("store", "boltdb", "registered store name, [memory, goleveldb, boltdb]")
var testConnID uint64
var fileMode os.FileMode = 0600 // owner can read and write

var myBucket = []byte("tidb")

func testInsertSql() {
	//logutil.SetLevel("ERROR")
	c := &check.C{}
	s := SetUpSuite(c)
	s.tk.MustExec("use test_db")
	s.tk.MustExec("drop table if exists t")
	s.tk.MustExec("create table t(c1 varchar(255), c2 varchar(255))")
	for i := 1; i <= 1; i++ {
		s.tk.MustExec("insert into t values (?, ?)", "aaa", "bbb")
	}
	//r := s.tk.MustQuery("select * from t")
	//fmt.Println(r.Rows())
}
func testSelectSql() {
	c := &check.C{}
	s := SetUpSuite(c)
	s.tk.MustExec("use test_db")
	r := s.tk.MustQuery("select * from t")
	fmt.Println(r.Rows())
}

type testDBSuite struct {
	store      kv.Storage
	dom        *domain.Domain
	schemaName string
	tk         *testkit.TestKit
	s          session.Session
	lease      time.Duration
	autoIDStep int64
}

func SetUpSuite(c *check.C) *testDBSuite {
	var err error

	testleak.BeforeTest()
	s := &testDBSuite{}
	s.lease = 200 * time.Millisecond
	session.SetSchemaLease(s.lease)
	session.SetStatsLease(0)

	s.autoIDStep = autoid.GetStep()
	autoid.SetStep(5000)

	//	s.store, err = mockstore.NewMockTikvStore(mockstore.WithPath("/opt/tidbtest/testdb02"))
	s.store, err = mockstore.NewMockTikvStore(mockstore.WithPath(""))

	s.dom, err = session.BootstrapSession(s.store)
	s.s, err = session.CreateSession(s.store)

	s.schemaName = "test_db"
	//_, err = s.s.Execute(context.Background(),"create database test_db")

	s.tk = testkit.NewTestKit(c, s.store)
	if err != nil {
		fmt.Println(err)
	}
	return s
}

func MustNewMVCCStore() mocktikv.MVCCStore {
	mvccStore, err := mocktikv.NewMVCCLevelDB("")
	if err != nil {
		panic(err)
	}
	return mvccStore
}

func SetUpSuite2(c *check.C) *testDBSuite {
	var err error

	testleak.BeforeTest()
	s := &testDBSuite{}
	s.lease = 0
	session.SetSchemaLease(s.lease)
	session.SetStatsLease(0)

	s.autoIDStep = autoid.GetStep()
	autoid.SetStep(1)

	//mvccStore, err := mocktikv.NewMVCCLevelDB("")

	mvccStore, _ := mocktikv.NewMemDB("")

	s.store, err = mockstore.NewMockTikvStore(
		//mockstore.WithCluster(cluster),
		mockstore.WithMVCCStore(mvccStore),
	)

	s.dom, err = session.BootstrapSession(s.store)
	if err != nil {
		fmt.Println(err)
	}
	time.Sleep(1 * time.Second)
	s.s, err = session.CreateSession(s.store)

	//s.schemaName = "test_db"
	//_, err = s.s.Execute(context.Background(), "create database test_db")

	s.tk = testkit.NewTestKit(c, s.store)
	if err != nil {
		fmt.Println(err)
	}

	return s
}
func SetUpSuite3() {

	mvccStore, err := mocktikv.NewMemDB("")
	if err != nil {
		panic("mvccStore---->" + err.Error())
	}
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithMVCCStore(mvccStore),
	)
	if err != nil {
		panic("store---->" + err.Error())
	}
	_, err = session.BootstrapSession(store)
	if err != nil {
		panic("BootstrapSession---->" + err.Error())
	}
}
func call(skip int) {
	pc, file, line, _ := runtime.Caller(skip)
	pcName := runtime.FuncForPC(pc).Name() //获取函数名
	fmt.Println(fmt.Sprintf("File:%s  Line:%d  Func:%s", file, line, pcName))
}

func print() {
	call(1)
}

func testTable01() {
	c := &check.C{}
	s := SetUpSuite(c)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists t")
	s.tk.MustExec("create table t(c1 int, c2 int)")
	err := s.s.NewTxn(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test_db"), model.NewCIStr("t"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("table meta id--->", tb.Meta().ID)
	ctx := s.s
	rid, err := tb.AddRecord(ctx, types.MakeDatums(1, 1))
	fmt.Println("table row id--->", rid)
	if err != nil {
		fmt.Println(err)
		return
	}
	s.s.StmtCommit(nil)
	txn, err := s.s.Txn(true)
	if err != nil {
		fmt.Println(err)
		return
	}
	txn.Commit(context.Background())

	r := s.tk.MustQuery("select * from t")
	fmt.Println(r.Rows())
}

func testInsertSql2() {
	//logutil.SetLevel("ERROR")
	c := &check.C{}
	//s:=SetUpSuite2(c)
	//SetUpSuite2(c)
	s := SetUpSuite2(c)
	s.tk.MustExec("use test")
	s.tk.MustExec("drop table if exists t")
	s.tk.MustExec("create table t(c1 int primary key, c2 varchar(255))")
	s.tk.MustExec("create index index_t on t(c2)")

	s.tk.MustExec("insert into t values (?, ?)", 101, "1")
	s.tk.MustExec("insert into t values (?, ?)", 102, "2")
	s.tk.MustExec("insert into t values (?, ?)", 103, "3")

	//s.tk.MustExec("insert into t values (?, ?)", "b", "2")
	//s.tk.MustExec("insert into t values (?, ?)", "g", "3")
	//for i := 1; i <= 1; i++ {
	//	s.tk.MustExec("insert into t values (?, ?)", "a", "1")
	//}

	//	r := s.tk.MustQuery(`select * from t where c1='a' or c1='g'`)
	//	r := s.tk.MustQuery(`select * from t where c1='a'`)
	r := s.tk.MustQuery(`select * from t`)
	fmt.Println(r.Rows())
}
func test03() {
	var x uint64
	x = 100
	head := make([]byte, 8)

	binary.BigEndian.PutUint64(head, x)
	str := string(head)
	fmt.Println(str)
	buf := []byte(str)
	y := binary.BigEndian.Uint64(buf)
	fmt.Println(y)
}
func main() {
	//config.LoadSettings("/opt/test/config/test_case1.txt", nil)
	//memkv.GetRemoteDBProxy()
	//time.Sleep(10 * time.Second)
	//fmt.Println("启动服务.....")
	//formatter := &logrus.TextFormatter{}
	//formatter.ForceColors=true
	//formatter.TimestampFormat="2006-01-02 15:04:00"
	//formatter.DisableColors=false
	//formatter.DisableTimestamp=true
	//formatter.FullTimestamp=false
	//logrus.SetFormatter(formatter)
	////logrus.SetLevel(logrus.InfoLevel)
	//logrus.SetLevel(logrus.ErrorLevel)
	//testBoltDB()
	logutil.SetLevel("error")
	//testInsertSql2()
	SetUpSuite3()
	//test03()
	//fmt.Printf( " \x1b[%dm%s\x1b[0m=\n", 34, "aaa")
	//logutil.BgLogger().Error("hello")
	//print()

}
