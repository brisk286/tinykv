package standalone_storage

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
// StandAloneStorage 是用于单节点 TinyKV 实例的 `Storage` 的实现。 它不与其他节点通信，所有数据都存储在本地。
type StandAloneStorage struct {
	// Your Data Here (1).
	//地址
	addr string
	//数据库储存文件的路径
	path string
	//日志
	logger *log.Logger
	// storage engine
	engines *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	logger := log.New()
	logger.SetLevel(log.StringToLogLevel(conf.LogLevel))

	return &StandAloneStorage{
		logger: logger,
		addr:   conf.StoreAddr,
		path:   conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.logger.Infof("stand alone storage start...")
	//新建Engines实例
	s.engines = engine_util.NewEngines(
		//kvEngine *badger.DB
		engine_util.CreateDB(s.path, false),
		//raftEngine *badger.DB  不需要raft
		nil,
		//kvPath string
		s.path,
		//raftPath string
		"",
	)
	s.logger.Infof("storage start success")
	s.logger.Infof("store address: %s", s.addr)
	s.logger.Infof("db path: %s", s.path)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	//关闭StandAloneStorage中engines的Kv数据库DB
	err := s.engines.Kv.Close()
	if err != nil {
		s.logger.Errorf("close KV engine failed && err=%+v", err)
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	fmt.Println("use StandAloneStorage")
	return s.GetReader(), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	//批量写
	writeBatch := &engine_util.WriteBatch{}
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			writeBatch.DeleteCF(m.Cf(), m.Key())
		}
	}
	return s.engines.WriteKV(writeBatch)
}

type reader struct {
	//badgerDB事务
	txn *badger.Txn
}

func (r reader) GetCF(cf string, key []byte) ([]byte, error) {
	//从txn的db中拿value
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r reader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r reader) Close() {
	r.txn.Discard()
}

func (s *StandAloneStorage) GetReader() *reader {
	fmt.Println("use GetReader")
	return &reader{
		//用一个新的Txn
		txn: s.engines.Kv.NewTransaction(false),
	}
}
