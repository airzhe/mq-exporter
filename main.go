package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"

	gxsync "github.com/dubbogo/gost/sync"
	gxtime "github.com/dubbogo/gost/time"
	mqhole "github.com/michaelklishin/rabbit-hole/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const (
	maxWheelTimeSpan    = 900e9  // 900s, 15 minute
	timeSpan            = 1000e6 // 100ms
	taskPoolQueueLength = 128
	taskPoolQueueNumber = 16
	taskPoolSize        = 2000
)

var (
	cfgPath    string
	version    bool
	versionStr = "unknow"
	addr       string
	wheel      *gxtime.Wheel
	taskPool   *gxsync.TaskPool

	config   []configItem
	taskMap  sync.Map
	taskList = make(map[string]func(), 0)
)

type configItem struct {
	Apiurl  string
	User    string
	Passwd  string
	Vhost   string
	Monitor map[string]mqMetrics
}

type mqMetrics struct {
	QueueName string `yaml:"queue"`
	Desc      string
}

func init() {
	//读取命令行参数
	flag.StringVar(&cfgPath, "config", "./config/config.yaml", "config file path")
	flag.BoolVar(&version, "v", false, "version")
	flag.StringVar(&addr, "addr", ":8082", "The address to listen on for HTTP requests.")

	buckets := maxWheelTimeSpan / timeSpan
	wheel = gxtime.NewWheel(time.Duration(timeSpan), int(buckets)) //wheel longest span is 15 minute
	taskPool = gxsync.NewTaskPool(
		gxsync.WithTaskPoolTaskQueueNumber(taskPoolQueueNumber), //tQNumber 切片长度16，类型是chan类型
		gxsync.WithTaskPoolTaskQueueLength(taskPoolQueueLength), //tQLen chan缓冲128
		gxsync.WithTaskPoolTaskPoolSize(taskPoolSize),           //tQPoolSize 起2000个worker消费，通过taskPoolSize%taskPoolQueueNumber消费对应的channel
	)
}

func main() {
	//解析命令行参数
	flag.Parse()
	if version == true {
		fmt.Println(versionStr)
		os.Exit(0)
	}
	//解析配置文件
	file, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		logrus.Fatalf("Read config fail:%s", err)
	}
	err = yaml.Unmarshal(file, &config)
	if err != nil {
		logrus.Fatalf("Fatal error config file read fail:%s", err)
	}
	//prometheus rgistry
	registry := prometheus.DefaultRegisterer
	for i := 0; i < len(config); i++ {
		item := config[i]
		//
		for k, v := range item.Monitor {
			//记录指标监控运行状态，0运行，1,暂停，2停止(后续实现此功能)
			taskMap.Store(k, 0)
			//创建metrics
			gaugeMetrics := prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: k,
					Help: v.Desc,
				},
				[]string{"attr"},
			)
			registry.MustRegister(gaugeMetrics)
			//需要在此处定义队列名，函数内部使用v.QueueName会被修改
			queueName := v.QueueName
			//必包形式引用了父级变量
			taskList[k] = func() {
				rmqc, err := mqhole.NewClient(item.Apiurl, item.User, item.Passwd)
				if err != nil {
					logrus.Error(err)
					return
				}
				//q是指针类型存在并发问题
				q, err := rmqc.GetQueue(item.Vhost, queueName)
				if err != nil {
					logrus.Errorf("%s %s %s %v", item.Apiurl, item.Vhost, queueName, err)
					return
				}
				//fmt.Printf("%#v\n", q)
				gaugeMetrics.WithLabelValues("message_ready").Set(float64(q.MessagesReady))
				gaugeMetrics.WithLabelValues("message_unacknowledged").Set(float64(q.MessagesUnacknowledged))
				gaugeMetrics.WithLabelValues("message_publish").Set(float64(q.MessageStats.Publish))
			}
		}
	}

	//metrics server && pprof
	go func() {
		fmt.Println("server run...")
		http.Handle("/metrics", promhttp.Handler())
		logrus.Fatal(http.ListenAndServe(addr, nil))
	}()

	//执行任务
	for {
		select {
		case <-wheel.After(5 * time.Second):
			taskMap.Range(func(k, v interface{}) bool {
				taskPool.AddTask(taskList[k.(string)])
				return true
			})
		}
	}
}