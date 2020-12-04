# mq-exporter 
通过 rabbitmq api 获取队列长度输出 metrics 指标给 prometheus 采集

包含 public（消息计数器），ready（积压消息）, unacknowledged（处理中未返回ack） 三个属性值
