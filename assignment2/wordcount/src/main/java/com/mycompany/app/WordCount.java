package com.mycompany.app;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCount extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    Map<String, Integer> counts = null;
    OutputCollector collector;
    
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple input) {
        WordCountTopology.ackCounter.inc();
        String word = input.getStringByField("word");
        Integer count = counts.get(word);
        if (count == null) {
            count = 0;
        }
        count++;
        this.counts.put(word, count);
        this.collector.emit(new Values(word, count));
        WordCountTopology.emitCounter.inc();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}