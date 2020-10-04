package mocktikv

import (
	"bytes"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/xp/shorttext-db/memkv"
	"go.uber.org/zap"
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
	mvccDB.db, err = memkv.NewDBProxy(1, 0)
	return mvccDB, err
}
func (mvcc *MVCCMemDB) Get(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) ([]byte, error) {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	return mvcc.getValue(key, startTS, isoLevel, resolvedLocks)
}

func (mvcc *MVCCMemDB) getValue(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) ([]byte, error) {
	//startKey := mvccEncode(key, lockVer)
	//iter :=  mvcc.db.Get(startKey)

	iter := mvcc.db.NewIterator(key)

	return getValueEx(iter, key, startTS, isoLevel, resolvedLocks)
}

func (mvcc *MVCCMemDB) Scan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLock []uint64) []Pair {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	iter, currKey, err := newScanIteratorEx(mvcc.db, startKey, endKey)
	if err != nil {
		logutil.BgLogger().Error("scan new iterator fail", zap.Error(err))
		return nil
	}

	ok := true
	var pairs []Pair
	for len(pairs) < limit && ok {
		value, err := getValueEx(iter, currKey, startTS, isoLevel, resolvedLock)
		if err != nil {
			pairs = append(pairs, Pair{
				Key: currKey,
				Err: errors.Trace(err),
			})
		}
		if value != nil {
			pairs = append(pairs, Pair{
				Key:   currKey,
				Value: value,
			})
		}

		skip := skipDecoderEx{currKey}
		ok, err = skip.Decode(iter)
		if err != nil {
			logutil.BgLogger().Error("seek to next key error", zap.Error(err))
			break
		}
		currKey = skip.currKey
	}
	return pairs
}

func (mvcc *MVCCMemDB) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) []Pair {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	iter := mvcc.db.NewDescendIterator(startKey, endKey)
	succ := iter.Valid()
	var (
		err     error
		currKey []byte
	)
	if succ {
		currKey, _, err = mvccDecode(iter.Key())
	}
	// TODO: return error.
	terror.Log(errors.Trace(err))
	helper := reverseScanHelper{
		startTS:       startTS,
		isoLevel:      isoLevel,
		currKey:       currKey,
		resolvedLocks: resolvedLocks,
	}

	for succ && len(helper.pairs) < limit {
		key, ver, err := mvccDecode(iter.Key())
		if err != nil {
			break
		}
		if bytes.Compare(key, startKey) < 0 {
			break
		}

		if !bytes.Equal(key, helper.currKey) {
			helper.finishEntry()
			helper.currKey = key
		}
		if ver == lockVer {
			var lock mvccLock
			err = lock.UnmarshalBinary(iter.Value())
			helper.entry.lock = &lock
		} else {
			var value mvccValue
			err = value.UnmarshalBinary(iter.Value())
			helper.entry.values = append(helper.entry.values, value)
		}
		if err != nil {
			logutil.BgLogger().Error("unmarshal fail", zap.Error(err))
			break
		}
		succ = iter.Prev()
	}
	if len(helper.pairs) < limit {
		helper.finishEntry()
	}
	return helper.pairs
}

func (mvcc *MVCCMemDB) BatchGet(ks [][]byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) []Pair {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	pairs := make([]Pair, 0, len(ks))
	for _, k := range ks {
		v, err := mvcc.getValue(k, startTS, isoLevel, resolvedLocks)
		if v == nil && err == nil {
			continue
		}
		pairs = append(pairs, Pair{
			Key:   k,
			Value: v,
			Err:   errors.Trace(err),
		})
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
	forUpdateTS := req.GetForUpdateTs()
	ttl := req.LockTtl
	minCommitTS := req.MinCommitTs
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	anyError := false
	batch := memkv.NewBatch()

	errs := make([]error, 0, len(mutations))
	txnSize := req.TxnSize
	for i, m := range mutations {
		// If the operation is Insert, check if key is exists at first.
		var err error
		// no need to check insert values for pessimistic transaction.
		//	fmt.Println("###xp->",m.Key)
		//str := m.Key[9:11]
		//if string(str) == "_i"{
		//	fmt.Println("###xp->",m.Key)
		//}
		//
		//	if string(str) == "_r"{
		//		fmt.Println("###xp->",m.Key)
		//	}
		op := m.GetOp()
		if (op == kvrpcpb.Op_Insert || op == kvrpcpb.Op_CheckNotExists) && forUpdateTS == 0 {
			v, err := mvcc.getValue(m.Key, startTS, kvrpcpb.IsolationLevel_SI, req.Context.ResolvedLocks)
			if err != nil {
				errs = append(errs, err)
				anyError = true
				continue
			}
			if v != nil {
				err = &ErrKeyAlreadyExist{
					Key: m.Key,
				}
				errs = append(errs, err)
				anyError = true
				continue
			}
		}
		if op == kvrpcpb.Op_CheckNotExists {
			continue
		}
		isPessimisticLock := len(req.IsPessimisticLock) > 0 && req.IsPessimisticLock[i]
		err = prewriteMutationEx(mvcc.db, batch, m, startTS, primary, ttl, txnSize, isPessimisticLock, minCommitTS)
		errs = append(errs, err)
		if err != nil {
			anyError = true
		}
	}
	if anyError {
		return errs
	}
	if err := mvcc.db.Write(batch); err != nil {
		return []error{err}
	}

	return errs
}

func (mvcc MVCCMemDB) Commit(keys [][]byte, startTS, commitTS uint64) error {
	mvcc.mu.Lock()
	defer func() {
		mvcc.mu.Unlock()
	}()

	batch := memkv.NewBatch()
	for _, k := range keys {
		err := commitKeyEx(mvcc.db, batch, k, startTS, commitTS)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return mvcc.db.Write(batch)
}

func (mvcc *MVCCMemDB) Rollback(keys [][]byte, startTS uint64) error {
	mvcc.mu.Lock()
	defer func() {
		mvcc.mu.Unlock()
	}()

	batch := memkv.NewBatch()
	for _, k := range keys {
		err := rollbackKeyEx(mvcc.db, batch, k, startTS)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return mvcc.db.Write(batch)
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
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	iter, currKey, err := newScanIteratorEx(mvcc.db, startKey, endKey)

	if err != nil {
		return errors.Trace(err)
	}

	batch := memkv.NewBatch()
	for iter.Valid() {
		dec := lockDecoderEx{expectKey: currKey}
		ok, err := dec.Decode(iter)
		if err != nil {
			return errors.Trace(err)
		}
		if ok && dec.lock.startTS == startTS {
			if commitTS > 0 {
				err = commitLockEx(batch, dec.lock, currKey, startTS, commitTS)
			} else {
				err = rollbackLockEx(batch, currKey, startTS)
			}
			if err != nil {
				return errors.Trace(err)
			}
		}

		skip := skipDecoderEx{currKey: currKey}
		_, err = skip.Decode(iter)
		if err != nil {
			return errors.Trace(err)
		}
		currKey = skip.currKey
	}
	return mvcc.db.Write(batch)
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

	action = kvrpcpb.Action_NoAction

	iter := mvcc.db.NewIterator(primaryKey)

	if iter.Valid() {
		dec := lockDecoderEx{
			expectKey: primaryKey,
		}
		var ok bool
		ok, err = dec.Decode(iter)
		if err != nil {
			err = errors.Trace(err)
			return
		}
		// If current transaction's lock exists.
		if ok && dec.lock.startTS == lockTS {
			lock := dec.lock
			batch := memkv.NewBatch()

			// If the lock has already outdated, clean up it.
			if uint64(oracle.ExtractPhysical(lock.startTS))+lock.ttl < uint64(oracle.ExtractPhysical(currentTS)) {
				if err = rollbackLockEx(batch, primaryKey, lockTS); err != nil {
					err = errors.Trace(err)
					return
				}
				if err = mvcc.db.Write(batch); err != nil {
					err = errors.Trace(err)
					return
				}
				return 0, 0, kvrpcpb.Action_TTLExpireRollback, nil
			}

			// If the caller_start_ts is MaxUint64, it's a point get in the autocommit transaction.
			// Even though the MinCommitTs is not pushed, the point get can ingore the lock
			// next time because it's not committed. So we pretend it has been pushed.
			if callerStartTS == math.MaxUint64 {
				action = kvrpcpb.Action_MinCommitTSPushed

				// If this is a large transaction and the lock is active, push forward the minCommitTS.
				// lock.minCommitTS == 0 may be a secondary lock, or not a large transaction (old version TiDB).
			} else if lock.minCommitTS > 0 {
				action = kvrpcpb.Action_MinCommitTSPushed
				// We *must* guarantee the invariance lock.minCommitTS >= callerStartTS + 1
				if lock.minCommitTS < callerStartTS+1 {
					lock.minCommitTS = callerStartTS + 1

					// Remove this condition should not affect correctness.
					// We do it because pushing forward minCommitTS as far as possible could avoid
					// the lock been pushed again several times, and thus reduce write operations.
					if lock.minCommitTS < currentTS {
						lock.minCommitTS = currentTS
					}

					//writeKey := mvccEncode(primaryKey, lockVer)
					writeValue, err1 := lock.MarshalBinary()
					if err1 != nil {
						err = errors.Trace(err1)
						return
					}
					batch.Put(primaryKey, writeValue, lockVer)
					if err1 = mvcc.db.Write(batch); err1 != nil {
						err = errors.Trace(err1)
						return
					}
				}
			}

			return lock.ttl, 0, action, nil
		}

		// If current transaction's lock does not exist.
		// If the commit info of the current transaction exists.
		c, ok, err1 := getTxnCommitInfoEx(iter, primaryKey, lockTS)
		if err1 != nil {
			err = errors.Trace(err1)
			return
		}
		if ok {
			// If current transaction is already committed.
			if c.valueType != typeRollback {
				return 0, c.commitTS, action, nil
			}
			// If current transaction is already rollback.
			return 0, 0, kvrpcpb.Action_NoAction, nil
		}
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
		batch := memkv.NewBatch()
		if err1 := writeRollbackEx(batch, primaryKey, lockTS); err1 != nil {
			err = errors.Trace(err1)
			return
		}
		if err1 := mvcc.db.Write(batch); err1 != nil {
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
func prewriteMutationEx(db memkv.KVClient, batch *memkv.Batch,
	mutation *kvrpcpb.Mutation, startTS uint64,
	primary []byte, ttl uint64, txnSize uint64,
	isPessimisticLock bool, minCommitTS uint64) error {
	//startKey := mvccEncode(mutation.Key, lockVer)
	iter := db.NewIterator(mutation.Key)

	dec := lockDecoderEx{
		expectKey: mutation.Key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		if dec.lock.startTS != startTS {
			if isPessimisticLock {
				// NOTE: A special handling.
				// When pessimistic txn prewrite meets lock, set the TTL = 0 means
				// telling TiDB to rollback the transaction **unconditionly**.
				dec.lock.ttl = 0
			}
			return dec.lock.lockErr(mutation.Key)
		}
		if dec.lock.op != kvrpcpb.Op_PessimisticLock {
			return nil
		}
		// Overwrite the pessimistic lock.
		if ttl < dec.lock.ttl {
			// Maybe ttlManager has already set the lock TTL, don't decrease it.
			ttl = dec.lock.ttl
		}
		if minCommitTS < dec.lock.minCommitTS {
			// The minCommitTS has been pushed forward.
			minCommitTS = dec.lock.minCommitTS
		}
	} else {
		if isPessimisticLock {
			return ErrAbort("pessimistic lock not found")
		}
		_, err = checkConflictValueEx(iter, mutation, startTS, startTS, false)
		if err != nil {
			return err
		}
	}

	op := mutation.GetOp()
	if op == kvrpcpb.Op_Insert {
		op = kvrpcpb.Op_Put
	}
	lock := mvccLock{
		startTS: startTS,
		primary: primary,
		value:   mutation.Value,
		op:      op,
		ttl:     ttl,
		txnSize: txnSize,
	}
	// Write minCommitTS on the primary lock.
	if bytes.Equal(primary, mutation.GetKey()) {
		lock.minCommitTS = minCommitTS
	}

	//	writeKey := mvccEncode(mutation.Key, lockVer)
	writeValue, err := lock.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}

	batch.Put(mutation.Key, writeValue, lockVer)
	return nil
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
	iter := db.NewIterator(key)

	dec := lockDecoderEx{
		expectKey: key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok || dec.lock.startTS != startTS {
		// If the lock of this transaction is not found, or the lock is replaced by
		// another transaction, check commit information of this transaction.
		c, ok, err1 := getTxnCommitInfoEx(iter, key, startTS)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if ok && c.valueType != typeRollback {
			// c.valueType != typeRollback means the transaction is already committed, do nothing.
			return nil
		}
		return ErrRetryable("txn not found")
	}
	// Reject the commit request whose commitTS is less than minCommiTS.
	if dec.lock.minCommitTS > commitTS {
		return &ErrCommitTSExpired{
			kvrpcpb.CommitTsExpired{
				StartTs:           startTS,
				AttemptedCommitTs: commitTS,
				Key:               key,
				MinCommitTs:       dec.lock.minCommitTS,
			}}
	}

	if err = commitLockEx(batch, dec.lock, key, startTS, commitTS); err != nil {
		return errors.Trace(err)
	}
	return nil
}
func commitLockEx(batch *memkv.Batch, lock mvccLock, key []byte, startTS, commitTS uint64) error {
	var valueType mvccValueType
	if lock.op == kvrpcpb.Op_Put {
		valueType = typePut
	} else if lock.op == kvrpcpb.Op_Lock {
		valueType = typeLock
	} else {
		valueType = typeDelete
	}
	value := mvccValue{
		valueType: valueType,
		startTS:   startTS,
		commitTS:  commitTS,
		value:     lock.value,
	}
	//writeKey := mvccEncode(key, commitTS)
	writeValue, err := value.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	batch.Put(key, writeValue, commitTS)
	//batch.Delete(mvccEncode(key, lockVer))
	batch.Delete(key, lockVer)

	return nil
}

func getTxnCommitInfoEx(iter memkv.Iterator, expectKey []byte, startTS uint64) (mvccValue, bool, error) {
	for iter.Valid() {
		dec := valueDecoderEx{
			expectKey: expectKey,
		}
		ok, err := dec.Decode(iter)
		if err != nil || !ok {
			return mvccValue{}, ok, errors.Trace(err)
		}

		if dec.value.startTS == startTS {
			return dec.value, true, nil
		}
	}
	return mvccValue{}, false, nil
}

func newScanIteratorEx(db memkv.KVClient, startKey, endKey []byte) (memkv.Iterator, []byte, error) {
	iter := db.NewScanIterator(startKey, endKey)
	// newScanIterator must handle startKey is nil, in this case, the real startKey
	// should be change the frist key of the store.
	if len(startKey) == 0 && iter.Valid() {
		key, _, err := mvccDecode(iter.Key())
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		startKey = key
	}
	return iter, startKey, nil
}

func writeRollbackEx(batch *memkv.Batch, key []byte, startTS uint64) error {
	tomb := mvccValue{
		valueType: typeRollback,
		startTS:   startTS,
		commitTS:  startTS,
	}
	//writeKey := mvccEncode(key, startTS)
	writeValue, err := tomb.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	batch.Put(key, writeValue, startTS)
	return nil
}

func rollbackLockEx(batch *memkv.Batch, key []byte, startTS uint64) error {
	err := writeRollbackEx(batch, key, startTS)
	if err != nil {
		return err
	}
	//batch.Delete(mvccEncode(key, lockVer))
	batch.Delete(key, lockVer)
	return nil
}

func rollbackKeyEx(db memkv.KVClient, batch *memkv.Batch, key []byte, startTS uint64) error {

	iter := db.NewIterator(key)

	if iter.Valid() {
		dec := lockDecoderEx{
			expectKey: key,
		}
		ok, err := dec.Decode(iter)
		if err != nil {
			return errors.Trace(err)
		}
		// If current transaction's lock exist.
		if ok && dec.lock.startTS == startTS {
			if err = rollbackLockEx(batch, key, startTS); err != nil {
				return errors.Trace(err)
			}
			return nil
		}

		// If current transaction's lock not exist.
		// If commit info of current transaction exist.
		c, ok, err := getTxnCommitInfoEx(iter, key, startTS)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			// If current transaction is already committed.
			if c.valueType != typeRollback {
				return ErrAlreadyCommitted(c.commitTS)
			}
			// If current transaction is already rollback.
			return nil
		}
	}

	// If current transaction is not prewritted before.
	value := mvccValue{
		valueType: typeRollback,
		startTS:   startTS,
		commitTS:  startTS,
	}
	//writeKey := mvccEncode(key, startTS)
	writeValue, err := value.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	batch.Put(key, writeValue, startTS)
	return nil
}
