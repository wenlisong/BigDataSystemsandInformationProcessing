package com.mycompany.app;

import com.codahale.metrics.Counter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {
    private static final String READ_SPOUT_ID = "read-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";
    
    static Counter emitCounter;
    static Counter ackCounter;

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(READ_SPOUT_ID, new ReadFile(), 1);

        builder.setBolt(SPLIT_BOLT_ID, new SplitSentence(), 8).shuffleGrouping(READ_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, new WordCount(), 12).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_BOLT_ID, new Report()).globalGrouping(COUNT_BOLT_ID);

        Config conf = new Config();
        // conf.setDebug(true);
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class, 1);
        // LocalCluster cluster = new LocalCluster();
        // cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
        StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
        // Thread.sleep(30000);
        // cluster.shutdown();
    }
}
