package etcd

import (
	"LogAgent/common"
	"LogAgent/tailfile"
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)


var(
	client *clientv3.Client
)
func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd failed,err:%v", err)
		return
	}
	return
}
func GetConfig(logKey string)(logCfgList []common.LogConifg,err error){
	//get
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	gresp, err := client.Get(ctx, logKey)
	if err != nil {
		// handle error!
		log.Errorf("Get to etcd log config failed,err:%v", err)
		return
	}
	if len(gresp.Kvs)==0 {
		log.Error("Get to etcd log config failed,because the len is 0! ")
	}
	for _, v := range gresp.Kvs {
		log.Infof("key:%s,value:%s\n", v.Key, v.Value)
		err=json.Unmarshal(v.Value,&logCfgList)
		if err != nil {
			// handle error!
			log.Errorf("Unmarshal to log config entry failed,err:%v", err)
			return
		}
	}
	cancel()
	return
}
func WatchCfg(logKey string) {
	var newCfgs  []common.LogConifg
	wch:=client.Watch(context.Background(),logKey)
	for wresp := range wch {
		for _,evt:= range wresp.Events {
			log.Infof("WatchCfg-->  Type:%s,Key:%s,Value:%s\n", evt.Type, evt.Kv.Key, evt.Kv.Value)
			err := json.Unmarshal(evt.Kv.Value, &newCfgs)
			if err!=nil {
				log.Errorf("json Unmarshal new log config fail, err:%v",err)
				continue
			}
			tailfile.PutNewCfg(newCfgs)
		}
	}
}
