package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	//set kvpath and raftpath the same as CreateDB
	kvpath := filepath.Join(conf.DBPath, "kv")
	raftpath := filepath.Join(conf.DBPath, "raft")
	//prepare for NewEngines
	kvEngine := engine_util.CreateDB("kv", conf)
	raftEngine := engine_util.CreateDB("raft", conf)
	return &StandAloneStorage{engine_util.NewEngines(kvEngine, raftEngine, kvpath, raftpath)}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	var Txn = s.engine.Kv.NewTransaction(false)
	reader := ImplStorageReader{Txn}
	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var wb *engine_util.WriteBatch = new(engine_util.WriteBatch)
	for _, modifyOp := range batch {
		//initial WriteBatch
		wb.SetCF(modifyOp.Cf(), modifyOp.Key(), modifyOp.Value())
	}
	wb.SetSafePoint()
	err := s.engine.WriteKV(wb)
	if err != nil {
		return err
	}
	return nil
}

type ImplStorageReader struct {
	Txn *badger.Txn
}

func (reader *ImplStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	var txn *badger.Txn = reader.Txn
	val, err := engine_util.GetCFFromTxn(txn, cf, key)
	if err != nil {
		return nil, nil
	}
	return val, err
}

func (reader *ImplStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := reader.Txn
	return engine_util.NewCFIterator(cf, txn)
}

func (reader *ImplStorageReader) Close() {
	reader.Txn.Discard()
}
