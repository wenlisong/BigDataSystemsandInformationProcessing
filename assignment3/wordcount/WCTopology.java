package myapps;

// import kafka.streams.StreamsConfig;

import java.util.Arrays;
import java.util.Properties;
/**
 * wctopology
 */
public class WCTopology {
    public static void main(String[] args) throws Exception {
        // Config conf = new Config();
        // conf.setDebug(true);
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
 
        // StreamsBuilder builder = new StreamsBuilder();
        // KStream<String, String> textLines = builder.stream("kafkawc");
        // textLines.to("kafkawc-output");
        
        // final Topology topology = builder.build();
        // System.out.println(topology.describe());
        
        // TopologyBuilder builder = new TopologyBuilder();
        // builder.setSpout("sentences", kafkaSpout, 4);
        // builder.setBolt("split", new SplitSentenceBolt(), 2).shuffleGrouping("sentences");
        // builder.setBolt("count", new WordCountBolt(), 2).fieldsGrouping("split", new Fields("word"));

        // if (args != null && args.length > 0) {
        //     conf.setNumWorkers(3);

        //     StormSubmitter.submitTopologyWithProgressBar("wcstorm", conf, builder.createTopology());
        // } else {
        //     conf.setMaxTaskParallelism(3);

        //     LocalCluster cluster = new LocalCluster();
        //     cluster.submitTopology("wcstorm", conf, builder.createTopology());
        //     Thread.sleep(10000);
        //     cluster.shutdown();
        // }
    }
}
