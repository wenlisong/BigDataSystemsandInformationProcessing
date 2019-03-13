package com.mycompany.app;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentence extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        WordCountTopology.ackCounter.inc();
        String line = input.getStringByField("sentence");
        String[] words = line.split(" ");
        for (String word : words) {
            if (word.trim().isEmpty())
                continue;
            this.collector.emit(new Values(word.toLowerCase()));
            WordCountTopology.emitCounter.inc();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}