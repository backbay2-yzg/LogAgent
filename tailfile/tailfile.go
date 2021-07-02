package tailfile

import (
	"LogAgent/common"
	"LogAgent/kafka"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

var (
	cfgChan chan []common.LogConifg
)

type tailTask struct {
	path    string
	topic   string
	tailObj *tail.Tail
}

func newTailTask(path string, topic string) *tailTask {
	tt := &tailTask{
		path:  path,
		topic: topic,
	}
	return tt
}

func (tt *tailTask) Init() (err error) {
	config := tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 是否跟随
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件哪个位置开始读
		MustExist: false,                                // 文件不存在报错
		Poll:      true,                                 //
	}
	tt.tailObj, err = tail.TailFile(tt.path, config)
	if err != nil {
		log.Error("tailfile fileName err:%v", err)
		return
	}
	log.Infof("create a tail success,path:%s,topic:%s", tt.path, tt.topic)
	return
}

func (tt *tailTask) run() {
	log.Infof("run a tailtask success,path:%s,topic:%s", tt.path, tt.topic)
	for {
		line, ok := <-tt.tailObj.Lines
		if !ok {
			log.Errorf("tail file close reopen, path:%s\n", tt.path)
			//time.Sleep(100 * time.Millisecond)
			continue
		}
		if len(strings.Trim(line.Text, "\r")) == 0 {
			log.Info("空行跳过...")
			continue
		}
		log.Info("msg:", line.Text)
		// 构造一个消息
		msg := &sarama.ProducerMessage{}
		msg.Topic = tt.topic
		msg.Value = sarama.StringEncoder(line.Text)
		//放到channel中
		kafka.SendChanMsg(msg)
		time.Sleep(1000 * time.Millisecond)

	}

}

//tail相关
func Init(cfgs []common.LogConifg) (err error) {

	for i, cfg := range cfgs {
		log.Infof("cfg-No.%d,--> path:%s,topic:%s", i, cfg.Path, cfg.Topic)
		tt := newTailTask(cfg.Path, cfg.Topic)
		err = tt.Init()
		if err != nil {
			log.Errorf("tt create tailfile err:%v,path:%s,topic:%s", err, tt.path, tt.topic)
			continue
		}
		go tt.run()
	}
	cfgChan = make(chan []common.LogConifg)
	newCfgs := <-cfgChan
	log.Infof("get the new log config,new config is :%v",newCfgs)
	return
}

func PutNewCfg(cfgs []common.LogConifg) {
	cfgChan <- cfgs
}
