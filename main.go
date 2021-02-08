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

	"github.com/928799934/googleAuthenticator"
	gxsync "github.com/dubbogo/gost/sync"
	gxtime "github.com/dubbogo/gost/time"
	"github.com/hashicorp/consul/api"
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
	consulAddr string
	addr       string
	wheel      *gxtime.Wheel
	taskPool   *gxsync.TaskPool
	err        error

	config     []configItem
	configYaml []byte
	taskMap    sync.Map
	taskList   = make(map[string]func(), 0)

	secret string

	consulConfigKey = "mq-exporter/config"

	mu sync.Mutex
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
	flag.StringVar(&consulAddr, "consul", "", "consule addr")
	flag.BoolVar(&version, "v", false, "version")
	flag.StringVar(&addr, "addr", ":8082", "The address to listen on for HTTP requests.")
	flag.StringVar(&secret, "secret", "", "api requests auth secret.")

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
	if err := initConfig(); err != nil {
		return
	}
	//
	dispatchTask()
	//metrics server && pprof
	go func() {
		fmt.Println("server run...")
		http.Handle("/metrics", promhttp.Handler())
		http.Handle("/reload", http.HandlerFunc(reloadConfig))
		http.Handle("/exit", http.HandlerFunc(exit))
		logrus.Fatal(http.ListenAndServe(addr, nil))
	}()

	//执行任务
	for {
		select {
		case <-wheel.After(5 * time.Second):
			mu.Lock()
			taskMap.Range(func(k, v interface{}) bool {
				taskPool.AddTask(taskList[k.(string)])
				return true
			})
			mu.Unlock()

		}
	}
}

func initConfig() error {
	var err error
	if consulAddr == "" {
		configYaml, err = ioutil.ReadFile(cfgPath)
	} else {
		configYaml, err = getConfigByConsul(consulAddr, consulConfigKey)
	}
	if err != nil {
		logrus.Warnf("Read config fail:%s", err)
		return err
	}
	err = yaml.Unmarshal(configYaml, &config)
	if err != nil {
		logrus.Warnf("Unmarshal config fail:%s", err)
		return err
	}
	return nil
}

func dispatchTask() {
	mu.Lock()
	defer mu.Unlock()
	logrus.Info("dispatch task")
	taskMap = sync.Map{}
	//prometheus rgistry
	registry := prometheus.DefaultRegisterer
	for i := 0; i < len(config); i++ {
		item := config[i]
		//
		for k, v := range item.Monitor {
			desc := v.Desc
			//记录指标监控运行状态，0运行，1,暂停，2停止(后续实现此功能)
			taskMap.Store(k, 0)
			//创建metrics
			gaugeMetrics := prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: k,
					Help: desc,
				},
				[]string{"attr", "desc"},
			)
			registry.Unregister(gaugeMetrics)
			registry.Register(gaugeMetrics)
			//需要在此处定义队列名，函数内部使用v.QueueName会被修改
			queueName := v.QueueName

			//闭包形式引用了父级变量
			taskList[k] = func() {
				rmqc, err := mqhole.NewClient(item.Apiurl, item.User, item.Passwd)
				if err != nil {
					logrus.Error(err)
					return
				}
				q, err := rmqc.GetQueue(item.Vhost, queueName)
				if err != nil {
					logrus.Errorf("%s %s %s %v", item.Apiurl, item.Vhost, queueName, err)
					return
				}
				//fmt.Printf("%#v\n", q)
				gaugeMetrics.WithLabelValues("message_ready", desc).Set(float64(q.MessagesReady))
				gaugeMetrics.WithLabelValues("message_unacknowledged", desc).Set(float64(q.MessagesUnacknowledged))
				gaugeMetrics.WithLabelValues("message_publish", desc).Set(float64(q.MessageStats.Publish))
			}
		}
	}
}

func authCode(r *http.Request) (err error) {
	if secret != "" {
		code := r.URL.Query().Get("code")
		if code == "" {
			return fmt.Errorf("%s", "code empty!")
		}
		ga := googleAuthenticator.NewGAuth()
		ret, err := ga.VerifyCode(secret, code, 1)
		if err != nil {
			return err
		}
		if !ret {
			return fmt.Errorf("%s", "code auth fail!")
		}
	}
	return nil
}

// 重新加载配置文件
func reloadConfig(w http.ResponseWriter, r *http.Request) {
	err := authCode(r)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}

	logrus.Info("reload config")
	if err := initConfig(); err != nil {
		return
	}
	dispatchTask()
	fmt.Fprintln(w, "reload config success")
}

// 退出程序
func exit(w http.ResponseWriter, r *http.Request) {
	err := authCode(r)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}

	fmt.Fprintln(w, "bye bye!")
	logrus.Fatal("exit!")
}

func getConfigByConsul(addr string, key string) ([]byte, error) {
	var ret []byte

	client, err := api.NewClient(&api.Config{Address: addr})
	if err != nil {
		logrus.Error(err)
		return nil, err
	}

	// Get a handle to the KV API
	kv := client.KV()

	pair, _, err := kv.List(consulConfigKey, nil)
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	if len(pair) == 0 {
		return nil, fmt.Errorf("从%s获取%s值为nil", addr, key)
	}

	for i := 0; i < len(pair); i++ {
		ret = append(ret, pair[i].Value...)
		ret = append(ret, 13)
	}
	return ret, nil

	/*
		// Lookup the pair
		pair, _, err := kv.Get(key, nil)
		if err != nil {
			logrus.Error(err)
			return nil, err
		}
		if pair == nil {
			return nil, fmt.Errorf("从%s获取%s值为nil", addr, key)
		}

		return pair.Value, nil
	*/
}
