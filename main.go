package main

import (
	"LogAgent/common"
	"LogAgent/etcd"
	"LogAgent/kafka"
	"LogAgent/tailfile"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/ini.v1"
)

type Config struct {
	KafkaConfig `ini:"Kafka"`
	LogConfig   `ini:"Log"`
	EtcdConfig  `ini:"Etcd"`
}
type KafkaConfig struct {
	ServerIp    string `ini:"server-ip"`
	ServerPort  string `ini:"server-port"`
	ChannelSize int64  `ini:"channel-size"`
}
type EtcdConfig struct {
	ServerIp    string `ini:"server-ip"`
	ServerPort  string `ini:"server-port"`
	LogFilePathKey string  `ini:"log-file-path-key"`
}
type LogConfig struct {
	LogFilePath string `ini:"log-file-path"`
}

func readConfig() {
	//1.读取ini配置文件
	cfg, err := ini.Load("./conf/config.ini")
	if err != nil {
		log.Errorf("load config fail,err:%v", err)
		fmt.Printf("Fail to read file: %v", err)
		return
	}
	kafkaServerIp := cfg.Section("Kafka").Key("server-ip").String()
	kafkaServerPort := cfg.Section("Kafka").Key("server-port").String()
	logFilePath := cfg.Section("LogConfig").Key("log-file-path").String()
	fmt.Println("ip=", kafkaServerIp, " port=", kafkaServerPort)
	fmt.Println("logFilePath=", logFilePath)
}

var (
	configObj *Config
)

//日志收集的客户端
func main() {
	//1.读取ini配置文件
	configObj = new(Config)
	//测试一下能不能读取配置
	readConfig()
	//配置文件解析转化为结构体
	err := ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		log.Errorf("load config fail,err:%v", err)
		return
	}
	fmt.Printf("read file: %#v\n", configObj)

	//1.初始化etcd address
	address := configObj.EtcdConfig.ServerIp + ":" + configObj.EtcdConfig.ServerPort
	log.Info(address)
	//通过etcd获取log配置json
	err=etcd.Init([]string{address})
	if err != nil {
		log.Errorf("etcd init fail,err:%v", err)
		return
	}
	//获取log配置
	cfgs, err := etcd.GetConfig(configObj.EtcdConfig.LogFilePathKey)
	if err != nil {
		log.Errorf("etcd GetConfig fail,err:%v", err)
		return
	}
	for i, cfg := range cfgs {
		log.Infof("cfg-No.%d,--> path:%s,topic:%s",i,cfg.Path,cfg.Topic)

	}

	//观察log配置变化
	go etcd.WatchCfg(configObj.EtcdConfig.LogFilePathKey)

	//2.初始化kafka
	address = configObj.KafkaConfig.ServerIp + ":" + configObj.KafkaConfig.ServerPort
	log.Info(address)
	err = kafka.Init([]string{address}, configObj.ChannelSize)
	if err != nil {
		log.Errorf("kafka init fail,err:%v", err)
		return
	}
	log.Info("Kafka init Succeed")

	tailInit(cfgs)

	run()


}
func run() {
	select {

	}
}
func tailInit(cfgs []common.LogConifg) {
	//3.根据配置文件的日志路径初始化tail
	err := tailfile.Init(cfgs)
	if err != nil {
		log.Errorf("tailfile init fail,err:%v", err)
		return
	}
	log.Info("tailfile init Succeed")
}
