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
	engines *engine_util.Engines
	config  *config.Config
}

type StandALoneStorageReader struct {
	kvTxn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	//dbPath := conf.DBPath
	//kvPath := filepath.Join(dbPath, "kv")
	//raftPath := filepath.Join(dbPath, "raft")
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")

	kvEngine := engine_util.CreateDB(kvPath, true)
	raftEngine := engine_util.CreateDB(raftPath, false)
	return &StandAloneStorage{
		engines: engine_util.NewEngines(kvEngine,raftEngine,kvPath,""),
		config:  nil,
	}

}

func NewStandALoneStorageReader(kvTxn *badger.Txn) *StandALoneStorageReader{
	return &StandALoneStorageReader{kvTxn: kvTxn}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.engines.Kv.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engines.Kv.NewTransaction(false)
	return NewStandALoneStorageReader(txn),nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _ , entry := range batch{
		switch entry.Data.(type) {
		case storage.Put:
			put := entry.Data.(storage.Put)
			err := engine_util.PutCF(s.engines.Kv, put.Cf, put.Key, put.Value)
			if err != nil{
				return err
			}
		case storage.Delete:
			delete := entry.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.engines.Kv, delete.Cf, delete.Key)
			if err != nil{
				return err
			}
		}
	}
	return nil
}

func (sReader *StandALoneStorageReader) GetCF(cf string, key []byte) ([]byte, error){
	val, err := engine_util.GetCFFromTxn(sReader.kvTxn, cf, key)
	if err == badger.ErrKeyNotFound{
		return nil,nil
	}
	return val,err
}
func (sReader *StandALoneStorageReader) IterCF(cf string) engine_util.DBIterator{
	return engine_util.NewCFIterator(cf,sReader.kvTxn)
}

func (sReader *StandALoneStorageReader) Close(){
	sReader.kvTxn.Discard()
}