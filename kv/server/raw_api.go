package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)




// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil{
		return &kvrpcpb.RawGetResponse{},err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil{
		return &kvrpcpb.RawGetResponse{},err
	}
	resp  := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: false,
	}
	if value == nil{
		resp.NotFound = true
	}
	return resp,nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	modify := storage.Modify{Data: put}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return &kvrpcpb.RawPutResponse{}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delte := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	modify := storage.Modify{Data: delte}
	err := server.storage.Write(req.Context , []storage.Modify{modify})
	if err != nil{
		return &kvrpcpb.RawDeleteResponse{},err
	}
	return &kvrpcpb.RawDeleteResponse{} , nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	limit := req.Limit
	startKey := req.StartKey
	reader, err := server.storage.Reader(req.Context)
	if err != nil{
		return &kvrpcpb.RawScanResponse{} , err
	}
	iter := reader.IterCF(req.Cf)
	iter.Seek(startKey)
	kvs := make([]*kvrpcpb.KvPair,0)
	for ; iter.Valid() ; iter.Next(){
		item := iter.Item()
		val, _ := item.Value()
		kvs = append(kvs , &kvrpcpb.KvPair{
			Key:                  item.Key(),
			Value:                val,
		})
		limit --
		if limit == 0{
			break
		}
	}
	response := &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}
	return response, nil
}
