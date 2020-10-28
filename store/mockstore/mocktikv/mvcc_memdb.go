package mocktikv

import (
	"bytes"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/xp/shorttext-db/memkv"
	"math"
	"sync"
)

type MVCCMemDB struct {
	db memkv.KVClient
	mu sync.RWMutex
}

func NewMemDB(path string) (*MVCCMemDB, error) {
	mvccDB := &MVCCMemDB{}
	var err error
	mvccDB.db, err = memkv.NewDBProxy()
	kv.MyDB = mvccDB.db.(*memkv.DBProxy)
	return mvccDB, err
}
func (mvcc *MVCCMemDB) GetKVClient() memkv.KVClient {
	return mvcc.db
}
func (mvcc *MVCCMemDB) Get(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) ([]byte, error) {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	val, _ := mvcc.getValue(key, startTS, isoLevel, resolvedLocks)
	//xhelper.PrintKey3("MVCCMemDB-->Get",key,val)
	return val, nil
}

func (mvcc *MVCCMemDB) getValue(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) ([]byte, error) {
	value, ok := mvcc.db.GetByRawKey(key, startTS)
	if !ok {
		return nil, nil
	} else {
		if value.ValueType == uint32(typeRollback) ||
			value.ValueType == uint32(typeLock) || value.ValueType == uint32(typeDelete) {
			return nil, nil
		}
	}
	return value.Val, nil
}

//func (mvcc *MVCCMemDB) scan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLock []uint64,desc bool) []Pair {
//
//	var (
//		err error
//		currKey []byte
//	)
//	iter := mvcc.db.NewScanIterator(startKey,endKey,false,desc)
//	var pairs []Pair
//	for iter.Valid(){
//		currKey,_,err = mvccDecode(iter.Key())
//		if err != nil{
//			logutil.BgLogger().Error("scan new iterator fail", zap.Error(err))
//			if desc{
//				iter.Prev()
//			}else{
//				iter.Next()
//			}
//			continue
//		}
//		mvccVal := &mvccValue{}
//		err =mvccVal.UnmarshalBinary(iter.Value())
//		if err != nil {
//			logutil.BgLogger().Error("mvccVal unmarshalBinary fail", zap.Error(err))
//			//pairs = append(pairs, Pair{
//			//	Key: currKey,
//			//	Err: errors.Trace(err),
//			//})
//			if desc{
//				iter.Prev()
//			}else{
//				iter.Next()
//			}
//			continue
//		}
//		//如果查询的数据时间戳大于提交的时间，该数据无效
//		if mvccVal.commitTS > startTS{
//			if desc{
//				iter.Prev()
//			}else{
//				iter.Next()
//			}
//			continue
//		}
//		//删除、回滚、锁住的数据，均为无效
//		if mvccVal.valueType == typeRollback || mvccVal.valueType == typeLock || mvccVal.valueType ==typeDelete  {
//			if desc{
//				iter.Prev()
//			}else{
//				iter.Next()
//			}
//			continue
//		}
//		pairs = append(pairs, Pair{
//			Key:   currKey,
//			Value: mvccVal.value,
//		})
//		if len(pairs) >= limit{
//			break
//		}
//		if desc{
//			iter.Prev()
//		}else{
//			iter.Next()
//		}
//	}
//
//	return pairs
//}

func (mvcc *MVCCMemDB) Scan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLock []uint64) []Pair {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()
	result := mvcc.db.Scan(startKey, endKey, startTS, limit, false, nil)
	pairs := make([]Pair, 0, len(result))
	for _, item := range result {
		pairs = append(pairs, Pair{Key: item.RawKey, Value: item.Val})
	}
	return pairs
}

//func (mvcc *MVCCMemDB) getPair(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLock []uint64) (Pair,bool){
//	pairs := mvcc.scan(key,key,1,startTS,isoLevel,resolvedLock,false)
//	if len(pairs) >0{
//		return pairs[0],true
//	}else{
//		return Pair{},false
//	}
//}

func (mvcc *MVCCMemDB) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) []Pair {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()
	result := mvcc.db.Scan(startKey, endKey, startTS, limit, true, nil)
	pairs := make([]Pair, 0, len(result))
	for _, item := range result {
		pairs = append(pairs, Pair{Key: item.RawKey, Value: item.Val})
	}
	return pairs
}

func (mvcc *MVCCMemDB) BatchGet(ks [][]byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) []Pair {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()
	pairs := make([]Pair, 0, len(ks))
	for _, k := range ks {
		val, _ := mvcc.getValue(k, startTS, isoLevel, resolvedLocks)
		if len(val) == 0 {
			continue
		}
		pairs = append(pairs, Pair{Key: k, Value: val})
	}
	return pairs
}

func (mvcc MVCCMemDB) PessimisticLock(req *kvrpcpb.PessimisticLockRequest) *kvrpcpb.PessimisticLockResponse {
	panic("implement me")
}

func (mvcc *MVCCMemDB) PessimisticRollback(keys [][]byte, startTS, forUpdateTS uint64) []error {
	panic("implement me")
}

func (mvcc *MVCCMemDB) Prewrite(req *kvrpcpb.PrewriteRequest) []error {
	mutations := req.Mutations
	primary := req.PrimaryLock
	startTS := req.StartVersion

	ttl := req.LockTtl
	minCommitTS := req.MinCommitTs
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	anyError := false
	//batch := memkv.NewBatch()

	errs := make([]error, 0, len(mutations))
	txnSize := req.TxnSize
	for i, m := range mutations {

		var err error

		op := m.GetOp()

		if op == kvrpcpb.Op_CheckNotExists {
			continue
		}
		isPessimisticLock := len(req.IsPessimisticLock) > 0 && req.IsPessimisticLock[i]
		err = prewriteMutationEx(mvcc.db, nil, m, startTS, primary, ttl, txnSize, isPessimisticLock, minCommitTS)
		//if err != nil{
		//	xhelper.Print("Prewrite 188-->",m.Key,"-->",startTS,"-->",tikv.GetGID(),"-->",err.Error())
		//}
		errs = append(errs, err)
		if err != nil {
			anyError = true
		}
	}
	if anyError {
		return errs
	}
	//if err := mvcc.db.Write(batch); err != nil {
	//	return []error{err}
	//}

	return errs
}

func (mvcc MVCCMemDB) Commit(keys [][]byte, startTS, commitTS uint64) error {
	mvcc.mu.Lock()
	defer func() {
		mvcc.mu.Unlock()
	}()

	//batch := memkv.NewBatch()
	for _, k := range keys {
		//xhelper.PrintKey("Commit----------->", k)
		//xhelper.PrintKey2("Commit 212----------->",k)
		err := commitKeyEx(mvcc.db, nil, k, startTS, commitTS)
		if err != nil {
			panic(err)
			return errors.Trace(err)
		}
	}
	return nil
}

func (mvcc *MVCCMemDB) Rollback(keys [][]byte, startTS uint64) error {
	mvcc.mu.Lock()
	defer func() {
		mvcc.mu.Unlock()
	}()

	for _, k := range keys {
		err := rollbackKeyEx(mvcc.db, k, startTS)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (mvcc *MVCCMemDB) Cleanup(key []byte, startTS, currentTS uint64) error {
	panic("implement me")
}

func (mvcc *MVCCMemDB) ScanLock(startKey, endKey []byte, maxTS uint64) ([]*kvrpcpb.LockInfo, error) {
	panic("implement me")
}

func (mvcc *MVCCMemDB) TxnHeartBeat(primaryKey []byte, startTS uint64, adviseTTL uint64) (uint64, error) {
	panic("implement me")
}

func (mvcc *MVCCMemDB) ResolveLock(startKey, endKey []byte, startTS, commitTS uint64) error {
	panic("implement me")

	//iter := mvcc.db.NewScanIterator(startKey, endKey,true,false)
	//
	//mvcc.mu.Lock()
	//defer mvcc.mu.Unlock()
	//
	//var (
	//	err error
	//	currKey []byte
	//	lock *mvccLock
	//)
	//for iter.Valid() {
	//	currKey,_,err = mvccDecode(iter.Key())
	//	if err != nil{
	//		return errors.Trace(err)
	//	}
	//	lock = &mvccLock{}
	//	err = lock.UnmarshalBinary(iter.Value())
	//	if err != nil {
	//		return errors.Trace(err)
	//	}
	//	if  lock.startTS == startTS {
	//		if commitTS > 0 {
	//			err = commitLockEx(mvcc.db, lock, currKey, startTS, commitTS)
	//		} else {
	//			err = rollbackLockEx(mvcc.db, currKey, startTS)
	//		}
	//		if err != nil {
	//			return errors.Trace(err)
	//		}
	//	}
	//	iter.Next()
	//}
	//return err
}

func (mvcc *MVCCMemDB) BatchResolveLock(startKey, endKey []byte, txnInfos map[uint64]uint64) error {
	panic("implement me")
}

func (mvcc *MVCCMemDB) GC(startKey, endKey []byte, safePoint uint64) error {
	panic("implement me")
}

func (mvcc *MVCCMemDB) DeleteRange(startKey, endKey []byte) error {
	panic("implement me")
}

func (mvcc *MVCCMemDB) CheckTxnStatus(primaryKey []byte, lockTS, callerStartTS, currentTS uint64,
	rollbackIfNotExist bool) (ttl uint64, commitTS uint64, action kvrpcpb.Action, err error) {

	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()
	//xhelper.PrintKey2("CheckTxnStatus-------->",primaryKey)
	action = kvrpcpb.Action_NoAction
	var lock *memkv.DBItem
	lock, err = getMvccLock(mvcc.db, primaryKey)
	if err != nil {
		return 0, 0, action, err
	}
	if lock != nil {
		if lock.StartTS == lockTS {
			// If the lock has already outdated, clean up it.
			//如果超过存活期，就清理掉
			if uint64(oracle.ExtractPhysical(lock.StartTS))+lock.Ttl < uint64(oracle.ExtractPhysical(currentTS)) {
				if err = rollbackLockEx(mvcc.db, primaryKey, lockTS); err != nil {
					err = errors.Trace(err)
					return
				}
				return 0, 0, kvrpcpb.Action_TTLExpireRollback, nil
			}
		}
		// If the caller_start_ts is MaxUint64, it's a point get in the autocommit transaction.
		// Even though the MinCommitTs is not pushed, the point get can ingore the lock
		// next time because it's not committed. So we pretend it has been pushed.
		//如果是自动提交的事务，就直接加入到提交
		if callerStartTS == math.MaxUint64 {
			action = kvrpcpb.Action_MinCommitTSPushed

			// If this is a large transaction and the lock is active, push forward the minCommitTS.
			// lock.minCommitTS == 0 may be a secondary lock, or not a large transaction (old version TiDB).
		} else if lock.MinCommitTS > 0 {
			action = kvrpcpb.Action_MinCommitTSPushed
			// We *must* guarantee the invariance lock.minCommitTS >= callerStartTS + 1
			if lock.MinCommitTS < callerStartTS+1 {
				lock.MinCommitTS = callerStartTS + 1

				// Remove this condition should not affect correctness.
				// We do it because pushing forward minCommitTS as far as possible could avoid
				// the lock been pushed again several times, and thus reduce write operations.
				if lock.MinCommitTS < currentTS {
					lock.MinCommitTS = currentTS
				}

				lock.CommitTS = lockVer
				mvcc.db.Put(lock)
			}
		}
		return lock.Ttl, 0, action, nil
	}

	// If current transaction's lock does not exist.
	// If the commit info of the current transaction exists.
	c, ok, err1 := getTxnCommitInfoEx(mvcc.db, primaryKey, lockTS)
	if err1 != nil {
		err = errors.Trace(err1)
		return
	}
	if ok {
		// If current transaction is already committed.
		if c.ValueType != uint32(typeRollback) {
			return 0, c.CommitTS, action, nil
		}
		// If current transaction is already rollback.
		return 0, 0, kvrpcpb.Action_NoAction, nil
	}

	// If current transaction is not prewritted before, it may be pessimistic lock.
	// When pessimistic txn rollback statement, it may not leave a 'rollbacked' tombstone.

	// Or maybe caused by concurrent prewrite operation.
	// Especially in the non-block reading case, the secondary lock is likely to be
	// written before the primary lock.

	if rollbackIfNotExist {
		// Write rollback record, but not delete the lock on the primary key. There may exist lock which has
		// different lock.startTS with input lockTS, for example the primary key could be already
		// locked by the caller transaction, deleting this key will mistakenly delete the lock on
		// primary key, see case TestSingleStatementRollback in session_test suite for example
		if err1 := writeRollbackEx(mvcc.db, primaryKey, lockTS); err1 != nil {
			err = errors.Trace(err1)
			return
		}
		return 0, 0, kvrpcpb.Action_LockNotExistRollback, nil
	}

	return 0, 0, action, &ErrTxnNotFound{kvrpcpb.TxnNotFound{
		StartTs:    lockTS,
		PrimaryKey: primaryKey,
	}}
}

func (mvcc *MVCCMemDB) Close() error {
	panic("implement me")
}

type lockDecoderEx struct {
	lock      mvccLock
	expectKey []byte
}

// Decode decodes the lock value if current iterator is at expectKey::lock.
func (dec *lockDecoderEx) Decode(iter memkv.Iterator) (bool, error) {

	if !iter.Valid() {
		return false, nil
	}

	iterKey := iter.Key()
	key, ver, err := mvccDecode(iterKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !bytes.Equal(key, dec.expectKey) {
		return false, nil
	}
	if ver != lockVer {
		return false, nil
	}

	var lock mvccLock
	err = lock.UnmarshalBinary(iter.Value())
	if err != nil {
		return false, errors.Trace(err)
	}
	dec.lock = lock
	iter.Next()
	return true, nil
}

type valueDecoderEx struct {
	value     mvccValue
	expectKey []byte
}

// Decode decodes a mvcc value if iter key is expectKey.
func (dec *valueDecoderEx) Decode(iter memkv.Iterator) (bool, error) {
	if !iter.Valid() {
		return false, nil
	}

	key, ver, err := mvccDecode(iter.Key())
	if err != nil {
		return false, errors.Trace(err)
	}
	if !bytes.Equal(key, dec.expectKey) {
		return false, nil
	}
	if ver == lockVer {
		return false, nil
	}

	var value mvccValue
	err = value.UnmarshalBinary(iter.Value())
	if err != nil {
		return false, errors.Trace(err)
	}
	dec.value = value
	iter.Next()
	return true, nil
}

type skipDecoderEx struct {
	currKey []byte
}

// Decode skips the iterator as long as its key is currKey, the new key would be stored.
func (dec *skipDecoderEx) Decode(iter memkv.Iterator) (bool, error) {

	for iter.Valid() {
		key, _, err := mvccDecode(iter.Key())
		if err != nil {
			return false, errors.Trace(err)
		}
		if !bytes.Equal(key, dec.currKey) {
			dec.currKey = key
			return true, nil
		}
		iter.Next()
	}
	return false, nil
}

func getValueEx(iter memkv.Iterator, key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) ([]byte, error) {
	dec1 := lockDecoderEx{expectKey: key}
	ok, err := dec1.Decode(iter)
	if ok && isoLevel == kvrpcpb.IsolationLevel_SI {
		startTS, err = dec1.lock.check(startTS, key, resolvedLocks)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	dec2 := valueDecoderEx{expectKey: key}
	for iter.Valid() {
		ok, err := dec2.Decode(iter)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ok {
			break
		}

		value := &dec2.value
		if value.valueType == typeRollback || value.valueType == typeLock {
			continue
		}
		// Read the first committed value that can be seen at startTS.
		if value.commitTS <= startTS {
			if value.valueType == typeDelete {
				return nil, nil
			}
			return value.value, nil
		}
	}
	return nil, nil
}
func getMvccLock(db memkv.KVClient, key []byte) (*memkv.DBItem, error) {

	val, ok := db.Get(key, lockVer)
	if ok {
		return val, nil
	} else {
		return nil, nil
	}
}
func getMvccValue(db memkv.KVClient, key []byte, ts uint64) (*memkv.DBItem, error) {
	val, ok := db.Get(key, ts)
	if ok {
		return val, nil
	} else {
		return nil, nil
	}
}

func lockErr(l *memkv.DBItem, key []byte) error {
	return &ErrLocked{
		Key:         mvccEncode(key, lockVer),
		Primary:     l.RawKey,
		StartTS:     l.StartTS,
		ForUpdateTS: l.ForUpdateTS,
		TTL:         l.Ttl,
		TxnSize:     l.TxnSize,
		LockType:    kvrpcpb.Op(l.Op),
	}
}

func prewriteMutationEx(db memkv.KVClient, batch *memkv.Batch,
	mutation *kvrpcpb.Mutation, startTS uint64,
	primary []byte, ttl uint64, txnSize uint64,
	isPessimisticLock bool, minCommitTS uint64) error {
	//startKey := mvccEncode(mutation.Key, lockVer)
	var (
		lock *memkv.DBItem
		err  error
	)

	//xhelper.PrintKey2("prewriteMutationEx 563---->", mutation.Key)
	//
	//xhelper.Print("prewriteMutationEx 576-->",string(mutation.Key),"-->", mutation.Key,"-->",len(mutation.Value)," -->",mutation.Op.String())

	lock, err = getMvccLock(db, mutation.Key)
	if err != nil {
		return errors.Trace(err)
	}

	if lock != nil {
		if lock.StartTS != startTS {
			if isPessimisticLock {
				// NOTE: A special handling.
				// When pessimistic txn prewrite meets lock, set the TTL = 0 means
				// telling TiDB to rollback the transaction **unconditionly**.
				lock.Ttl = 0
			}
			return lockErr(lock, mutation.Key)
		}
		if lock.Op != uint32(kvrpcpb.Op_PessimisticLock) {
			return nil
		}
		// Overwrite the pessimistic lock.
		if ttl < lock.Ttl {
			// Maybe ttlManager has already set the lock TTL, don't decrease it.
			ttl = lock.Ttl
		}
		if minCommitTS < lock.MinCommitTS {
			// The minCommitTS has been pushed forward.
			minCommitTS = lock.MinCommitTS
		}
	} else {
		if isPessimisticLock {
			return ErrAbort("pessimistic lock not found")
		}
	}

	op := mutation.GetOp()
	if op == kvrpcpb.Op_Insert {
		op = kvrpcpb.Op_Put
	}
	lock = &memkv.DBItem{
		StartTS: startTS,
		RawKey:  mutation.Key,
		Val:     mutation.Value,
		Op:      uint32(op),
		Ttl:     ttl,
		TxnSize: txnSize,
	}
	lock.CommitTS = lockVer
	// Write minCommitTS on the primary lock.
	if bytes.Equal(primary, mutation.GetKey()) {
		lock.MinCommitTS = minCommitTS
	}
	err = db.Put(lock)
	return err
}

func checkConflictValueEx(iter memkv.Iterator, m *kvrpcpb.Mutation, forUpdateTS uint64, startTS uint64, getVal bool) ([]byte, error) {
	dec := &valueDecoderEx{
		expectKey: m.Key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		return nil, nil
	}

	// Note that it's a write conflict here, even if the value is a rollback one, or a op_lock record
	if dec.value.commitTS > forUpdateTS {
		return nil, &ErrConflict{
			StartTS:          forUpdateTS,
			ConflictTS:       dec.value.startTS,
			ConflictCommitTS: dec.value.commitTS,
			Key:              m.Key,
		}
	}

	needGetVal := getVal
	needCheckAssertion := m.Assertion == kvrpcpb.Assertion_NotExist
	needCheckRollback := true
	var retVal []byte
	// do the check or get operations within one iteration to make CI faster
	for ok {
		if needCheckRollback {
			if dec.value.valueType == typeRollback {
				if dec.value.commitTS == startTS {

					return nil, &ErrAlreadyRollbacked{
						startTS: startTS,
						key:     m.Key,
					}
				}
			}
			if dec.value.commitTS < startTS {
				needCheckRollback = false
			}
		}
		if needCheckAssertion {
			if dec.value.valueType == typePut || dec.value.valueType == typeLock {
				if m.Op == kvrpcpb.Op_PessimisticLock {
					return nil, &ErrKeyAlreadyExist{
						Key: m.Key,
					}
				}
			} else if dec.value.valueType == typeDelete {
				needCheckAssertion = false
			}
		}
		if needGetVal {
			if dec.value.valueType == typeDelete || dec.value.valueType == typePut {
				retVal = dec.value.value
				needGetVal = false
			}
		}
		if !needCheckAssertion && !needGetVal && !needCheckRollback {
			break
		}
		ok, err = dec.Decode(iter)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if getVal {
		return retVal, nil
	}
	return nil, nil
}

func commitKeyEx(db memkv.KVClient, batch *memkv.Batch, key []byte, startTS, commitTS uint64) error {
	//startKey := mvccEncode(key, lockVer)
	var (
		lock *memkv.DBItem
		err  error
	)

	lock, err = getMvccLock(db, key)
	//if lock != nil{
	//xhelper.Print("MVCCMemDB-->commitKeyEx 715-->",string(key),"-->", key,"-->",len(lock.Val)," -->",kvrpcpb.Op(lock.Op).String(),"-->",commitTS)
	//}else{
	//	xhelper.Print("MVCCMemDB-->commitKeyEx 717-->",string(key),"-->", key,"-->","锁对象为空")
	//
	//}

	if err != nil {
		return errors.Errorf("key[%v]获取mvccLock失败:%s", key, err.Error())
	}

	if lock == nil || lock.StartTS != startTS {
		// If the lock of this transaction is not found, or the lock is replaced by
		// another transaction, check commit information of this transaction.
		c, ok, err1 := getTxnCommitInfoEx(db, key, startTS)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if ok && c.ValueType != uint32(typeRollback) {
			// c.valueType != typeRollback means the transaction is already committed, do nothing.
			return nil
		}
		//xhelper.Print("MVCCMemDB-->commitKeyEx 729-->Key",[]byte(key))
		return ErrRetryable("txn not found")
	}
	// Reject the commit request whose commitTS is less than minCommiTS.
	if lock.MinCommitTS > commitTS {
		return &ErrCommitTSExpired{
			kvrpcpb.CommitTsExpired{
				StartTs:           startTS,
				AttemptedCommitTs: commitTS,
				Key:               key,
				MinCommitTs:       lock.MinCommitTS,
			}}
	}

	if err = commitLockEx(db, lock, key, startTS, commitTS); err != nil {
		return errors.Trace(err)
	}
	return nil
}
func commitLockEx(db memkv.KVClient, lock *memkv.DBItem, key []byte, startTS, commitTS uint64) error {
	var valueType mvccValueType
	if lock.Op == uint32(kvrpcpb.Op_Put) {
		valueType = typePut
	} else if lock.Op == uint32(kvrpcpb.Op_Lock) {
		valueType = typeLock
	} else {
		valueType = typeDelete
	}
	value := &memkv.DBItem{
		ValueType: uint32(valueType),
		StartTS:   startTS,
		CommitTS:  commitTS,
		Val:       lock.Val,
		RawKey:    key,
	}

	db.Put(value)
	db.Delete(key, lockVer)
	//batch.Delete(mvccEncode(key, lockVer))

	return nil
}

//获取已经提交的信息
func getTxnCommitInfoEx(db memkv.KVClient, expectKey []byte, startTS uint64) (*memkv.DBItem, bool, error) {
	mvccValue, err := getMvccValue(db, expectKey, startTS)
	if err != nil {
		return nil, false, err
	}
	if mvccValue == nil {
		return nil, false, nil
	}
	return mvccValue, true, nil
}

//func newScanIteratorEx(db memkv.KVClient, startKey, endKey []byte) (memkv.Iterator, []byte, error) {
//	iter := db.NewScanIterator(startKey, endKey)
//	// newScanIterator must handle startKey is nil, in this case, the real startKey
//	// should be change the frist key of the store.
//	if len(startKey) == 0 && iter.Valid() {
//		key, _, err := mvccDecode(iter.Key())
//		if err != nil {
//			return nil, nil, errors.Trace(err)
//		}
//		startKey = key
//	}
//	return iter, startKey, nil
//}

func writeRollbackEx(db memkv.KVClient, key []byte, startTS uint64) error {
	tomb := &memkv.DBItem{
		ValueType: uint32(typeRollback),
		StartTS:   startTS,
		CommitTS:  startTS,
		RawKey:    key,
	}

	db.Put(tomb)
	return nil
}

func rollbackLockEx(db memkv.KVClient, key []byte, startTS uint64) error {
	err := writeRollbackEx(db, key, startTS)
	if err != nil {
		return err
	}
	//batch.Delete(mvccEncode(key, lockVer))
	db.Delete(key, startTS)
	return nil
}

func rollbackKeyEx(db memkv.KVClient, key []byte, startTS uint64) error {

	lock, err := getMvccLock(db, key)
	if err != nil {
		return err
	}
	if lock == nil {
		return nil
	}

	if lock.StartTS == startTS {
		if err = rollbackLockEx(db, key, startTS); err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	c, ok, err := getTxnCommitInfoEx(db, key, startTS)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		// If current transaction is already committed.
		if c.ValueType != uint32(typeRollback) {
			return ErrAlreadyCommitted(c.CommitTS)
		}
		// If current transaction is already rollback.
		return nil
	}

	// If current transaction is not prewritted before.
	value := &memkv.DBItem{
		ValueType: uint32(typeRollback),
		StartTS:   startTS,
		CommitTS:  startTS,
		RawKey:    key,
	}

	db.Put(value)
	return nil
}
