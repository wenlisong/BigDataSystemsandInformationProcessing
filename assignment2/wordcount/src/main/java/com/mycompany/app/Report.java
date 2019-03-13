package com.mycompany.app;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Report extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private HashMap<String, Integer> counts = null;
    private String outputFile = "/home/ubuntu/output-word-count.txt";
    private String outputCountFile = "/home/ubuntu/count.txt";
    private OutputCollector collector;
    
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
        this.counts = new HashMap<String, Integer>();
        this.collector = collector;
	}

    public void execute(Tuple tuple) {
        WordCountTopology.ackCounter.inc();
		String word = tuple.getStringByField("word");
		Integer count = tuple.getIntegerByField("count");
        this.counts.put(word, count);

        System.out.println(word + "\t" + this.counts.get(word));
        System.out.println("Emit count:\t" + WordCountTopology.emitCounter.getCount());
        System.out.println("Ack count:\t" + WordCountTopology.ackCounter.getCount());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("word", "count"));
	}

    public void cleanup() {
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        try {
            PrintWriter out = new PrintWriter(outputFile);
            for (String key : keys) {
                out.println(key + "\t" + this.counts.get(key));
            }
            out.close();
            out = new PrintWriter(outputCountFile);
            out.println("Emit count:");
            Long emitCount = WordCountTopology.emitCounter.getCount();
            out.println(emitCount);
            out.println("Ack count:");
            Long ackCount = WordCountTopology.ackCounter.getCount();
            out.println(ackCount);
            out.println("Fail count:");
            out.println(emitCount - ackCount);
            out.close();
        } catch (Exception e) {

        }
	}
}