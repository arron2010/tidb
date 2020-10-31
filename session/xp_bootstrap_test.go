package session

import (
	"fmt"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/logutil"
	"strconv"
	"strings"
	"testing"
	"time"
)

func BenchmarkBootstrapSession(b *testing.B) {
	logutil.SetLevel("error")
	mvccStore, _ := mocktikv.NewMemDB("")
	store, _ := mockstore.NewMockTikvStore(
		mockstore.WithMVCCStore(mvccStore),
	)

	//s, _ := session.CreateSession(store)
	//
	//mustExecute(s,"use test")
	//mustExecute(s,"drop table if exists t")
	//mustExecute(s,"create table t(c1 int primary key, c2 varchar(255))")
	//mustExecute(s,"create index index_t on t(c2)")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		BootstrapSession(store)
	}
}

func TestBootstrapSession3(t *testing.T) {
	logutil.SetLevel("error")

	logutil.SetLevel("error")
	mvccStore, _ := mocktikv.NewMemDB("")
	store, _ := mockstore.NewMockTikvStore(
		mockstore.WithMVCCStore(mvccStore),
	)
	BootstrapSession(store)
	s, _ := CreateSession(store)

	mustExecute(s, "use test")
	mustExecute(s, "drop table if exists t")
	mustExecute(s, "create table t(c1 int primary key, c2 int)")
	mustExecute(s, "create index index_t on t(c2)")

	mustExecute(s, "insert into t values (1, 2)")
	mustExecute(s, "insert into t values (3, 4)")
	mustExecute(s, "insert into t values (5, 6)")
	//c := &check.C{}
	//tk := testkit.NewTestKit(c, store)
	//r := tk.MustQuery(`select * from t`)
	//fmt.Println(r.Rows())
}
func TestBootstrapSession2(t *testing.T) {
	var s *session
	mvccStore, err := mocktikv.NewMemDB("")

	//mvccStore, err := mocktikv.NewMVCCLevelDB("")
	if err != nil {
		panic(err)
	}
	store, err := mockstore.NewMockTikvStore(
		//mockstore.WithCluster(cluster),
		mockstore.WithMVCCStore(mvccStore),
	)

	s, err = createSession(store)
	if err != nil {
		panic(err)
	}
	//time.Sleep(3* time.Second)
	fmt.Println("-----------------TestBootstrapSession2-------------->doDDLWorks:", s.String())
	s.SetValue(sessionctx.Initing, true)
	doDDLWorks(s)
	//mustExecute(s, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", mysql.SystemDB))
	//mustExecute(s, CreateUserTable)
	s.ClearValue(sessionctx.Initing)

	//109 68 68 76 74 111 98 76 105 255 115 116 0 0 0 0 0 0 249 0 0 0 0 0 0 0 76
	time.Sleep(3 * time.Second)
	printKey("109 68 68 76 74 111 98 76 105 255 115 116 0 0 0 0 0 0 249 0 0 0 0 0 0 0 76", mvccStore)
}

func printKey(str string, db *mocktikv.MVCCMemDB) {
	b := convertBytes(str)
	p := db.GetKVClient()
	list := p.FindByKey(b, false)
	for _, item := range list {
		fmt.Println("RawKey:", item.RawKey, " CommitTS:", item.CommitTS, " Value:", len(item.Val), " :ValueType", item.ValueType)
	}
}
func convertBytes(str string) []byte {
	chars := strings.Split(str, " ")
	buf := make([]byte, 0, len(chars))
	for i := 0; i < len(chars); i++ {
		b, _ := strconv.ParseUint(chars[i], 10, 8)
		buf = append(buf, byte(b))
	}
	return buf
}

//func TestBootstrapSession(t *testing.T) {
//
//	mvccStore,err := mocktikv.NewMemDB("")
//	if err != nil{
//		panic(err)
//	}
//	store, err := mockstore.NewMockTikvStore(
//		//mockstore.WithCluster(cluster),
//		mockstore.WithMVCCStore(mvccStore),
//	)
//	if err != nil{
//		panic(err)
//	}
//	se, err := CreateSession4Test(store)
//	if err != nil{
//		panic(err)
//	}
//	id := atomic.AddUint64(&testConnID, 1)
//	se.SetConnectionID(id)
//	//dbName :="test"
//	mustExecute(se, CreateGlobalVariablesTable)
//}
