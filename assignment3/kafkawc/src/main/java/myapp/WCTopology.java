package myapp;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

public class WCTopology {

    public static final String KAFKA_SPOUT = "kafka-spout";
    public static final String ID_SPLIT_BOLT = "split-sentence-bolt";
    public static final String ID_COUNT_BOLT = "word-count-bolt";


    public static final int KAFKA_SPOUT_COUNT = 1;
    public static final int SPLIT_BOLT_COUNT = 3;
    public static final int COUNT_BOLT_COUNT = 3;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpoutConfig<String, String> config =
                KafkaSpoutConfig.builder("10.142.0.2:9092", "wordcount").build();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout<String, String>(config), KAFKA_SPOUT_COUNT);

        builder.setBolt(ID_SPLIT_BOLT, new SplitSentenceBolt(), SPLIT_BOLT_COUNT).shuffleGrouping(KAFKA_SPOUT);
        builder.setBolt(ID_COUNT_BOLT, new WordCountBolt(), COUNT_BOLT_COUNT).globalGrouping(ID_SPLIT_BOLT);

        Config conf = new Config();
        conf.setDebug(false);

        try {
            if (args != null && args.length > 0) {
                conf.setNumWorkers(2);
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(3);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("word-count", conf, builder.createTopology());

                Thread.sleep(60 * 1000);

                cluster.shutdown();
            }
        } catch (Exception e) {
            System.out.println("submit failed with error:" + e.toString());
        }
    }

}
