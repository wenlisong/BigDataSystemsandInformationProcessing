package com.mycompany.app;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.storm.topology.TopologyBuilder;
// import org.apache.storm.kafk

/**
 * wctopology
 */
public class WCTopology {
    public static void main(String[] args) throws Exception {
        // Config conf = new Config();
        // conf.setDebug(true);
        
        // Properties props = new Properties();
        // props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        // props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
 
        // StreamsBuilder builder = new StreamsBuilder();
        // KStream<String, String> textLines = builder.stream("kafkawc");
        // textLines.to("kafkawc-output");
        
        // final Topology topology = builder.build();
        // System.out.println(topology.describe());
        
        //创建TopologyBuilder
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        
        KafkaSpoutConfig.Builder<String, String> kafkaSpoutConfigBuilder;
        //kafka连接信息
     String bootstrapServers="192.168.1.86:9092,192.168.1.87:9093,192.168.1.88:9094";
        //主题
        String topic = "test";
        /**
         * 构造kafkaSpoutConfigBuilder构造器
         *
         * bootstrapServers:    Kafka链接地址 ip:port
         * StringDeserializer:  key Deserializer    主题key的反序列化
         * StringDeserializer:  value Deserializer  主题的value的反序列化
         * topic: 主题名称
         */
        kafkaSpoutConfigBuilder = new KafkaSpoutConfig.Builder<>(
                bootstrapServers,
                StringDeserializer.class,
                StringDeserializer.class,
                topic);

        //使用kafkaSpoutConfigBuilder构造器构造kafkaSpoutConfig,并配置相应属性
        KafkaSpoutConfig<String, String> kafkaSpoutConfig = kafkaSpoutConfigBuilder
                /**
                 * 设置groupId
                 */
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, topic.toLowerCase() + "_storm_group")

                /**
                 * 设置session超时时间,该值应介于
                 * [group.min.session.timeout.ms, group.max.session.timeout.ms] [6000,300000]
                 * 默认值:10000
                 */
                .setProp(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "100000")

                /**
                 * 设置拉取最大容量
                 */
                .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576")

                /**
                 * 设置控制客户端等待请求响应的最大时间量
                 * 默认值:30000
                 */
                .setProp(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "300000")

                /**
                 * 设置心跳到消费者协调器之间的预期时间。
                 * 心跳用于确保消费者的会话保持活动并且当新消费者加入或离开组时促进重新平衡
                 * 默认值:	3000        (一般设置低于session.timeout.ms的三分之一)
                 */
                .setProp(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "30000")

                /**
                 * 设置offset提交时间15s  默认30s
                 */
                .setOffsetCommitPeriodMs(15000)

                /**
                 * 设置拉取最大在session超时时间内最好处理完成的个数
                 */
                .setMaxPollRecords(20)

                /**
                 * 设置拉取策略
                 */
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST)

                /**
                 * 构造kafkaSpoutConfig
                 */
                .build();

        //setSpout
        topologyBuilder.setSpout("kafkaSpout",new KafkaSpout(kafkaSpoutConfig));

        //setbolt
        topologyBuilder.setBolt("KafkaSpoutBolt", new KafkaSpoutBolt()).localOrShuffleGrouping("kafkaSpout");

        Config config = new Config();
        /**
         * 设置supervisor和worker之间的通信超时时间.
         * 超过这个时间supervisor会重启worker  (秒)
         */
        config.put("supervisor.worker.timeout.secs",600000);
        /**
         * 设置storm和zookeeper之间的超时时间.
         */
        config.put("storm.zookeeper.session.timeout",1200000000);
        /**
         * 设置debug模式 日志输出更全
         * 只能在本地LocalCluster模式下启用
         */
        config.setDebug(true);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("NewKafKaTopo", config, topologyBuilder.createTopology());
        Utils.sleep(Long.MAX_VALUE);
        localCluster.shutdown();
    }
}
