package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
// RawGet 根据 RawGetRequest 的 CF 和 Key 字段返回对应的 Get 响应
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	log.Infof("RawGet cf=%v, key=%v", req.GetCf(), req.GetKey())
	//创建RawGetResponse
	rsp := new(kvrpcpb.RawGetResponse)
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		log.Errorf("RawGet failed && err=%+v", err)
		rsp.Error = err.Error()
		rsp.NotFound = true
		return rsp, nil
	}
	if len(val) == 0 {
		rsp.NotFound = true
	}
	rsp.Value = val
	return rsp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
// RawPut 将目标数据放入存储，并返回相应的响应
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	// 提示：考虑使用 Storage.Modify 来存储要修改的数据
	log.Infof("RawPut cf=%v, key=%v, value=%v", req.GetCf(), req.GetKey(), req.GetValue())
	rsp := new(kvrpcpb.RawPutResponse)
	modify := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	}
	//修改直接写入
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		log.Errorf("RawPut failed && err=%+v", err)
		rsp.Error = err.Error()
	}
	return rsp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
// RawDelete 从存储中删除目标数据并返回相应的响应
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	// 提示：考虑使用 Storage.Modify 存储要删除的数据
	log.Infof("RawDelete cf=%v, key=%v", req.GetCf(), req.GetKey())
	rsp := new(kvrpcpb.RawDeleteResponse)
	modify := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		log.Errorf("RawDelete failed && err=%+v", err)
		rsp.Error = err.Error()
	}
	return rsp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
// RawScan 从开始键开始扫描数据直到限制。 并返回相应的结果
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	log.Infof("RawScan cf=%v, start=%v, limit=%v", req.GetCf(), req.GetStartKey(), req.GetLimit())
	rsp := new(kvrpcpb.RawScanResponse)
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	//返回BadgerIterator， 它实现了DBIterator接口， 使用cf做前缀的迭代器
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	//在limit范围内返回范围队列
	for i := uint32(0); iter.Valid() && i < req.Limit; i += 1 {
		item := iter.Item()
		key := item.Key()
		val, err := item.Value()
		if err != nil {
			log.Warnf("RawScan items error occurs && key=%v, err=%+v", key, err)
		}
		rsp.Kvs = append(rsp.Kvs,
			&kvrpcpb.KvPair{
				Key:   key,
				Value: val,
			},
		)
		iter.Next()
	}
	return rsp, nil
}
