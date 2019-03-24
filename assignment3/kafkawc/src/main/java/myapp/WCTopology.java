package myapp;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

public class WCTopology {

    public static final String ID_KAFKA_SPOUT = "kafka-spout";
    public static final String ID_SPLIT_BOLT = "split-sentence-bolt";
    public static final String ID_COUNT_BOLT = "word-count-bolt";
    public static final int NUM_KAFKA_SPOUT = 1;
    public static final int NUM_SPLIT_BOLT = 3;
    public static final int NUM_COUNT_BOLT = 3;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpoutConfig<String, String> config =
                KafkaSpoutConfig.builder("ip-172-31-13-50:9092", "kafkawc").build();

        builder.setSpout(ID_KAFKA_SPOUT, new KafkaSpout<String, String>(config), NUM_KAFKA_SPOUT);

        builder.setBolt(ID_SPLIT_BOLT, new SplitSentenceBolt(), NUM_SPLIT_BOLT).shuffleGrouping(ID_KAFKA_SPOUT);
        builder.setBolt(ID_COUNT_BOLT, new WordCountBolt(), NUM_COUNT_BOLT).globalGrouping(ID_SPLIT_BOLT);

        Config conf = new Config();
        conf.setDebug(false);

        try {
            if (args != null && args.length > 0) {
                conf.setNumWorkers(4);
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(4);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("kafkawc", conf, builder.createTopology());
                Thread.sleep(1000);
                cluster.shutdown();
            }
        } catch (Exception e) {
        }
    }
}
