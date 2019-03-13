package com.mycompany.app;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFile extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ReadFile.class);

    SpoutOutputCollector collector;
    private FileReader fileReader;
    BufferedReader br;
    private String fileName = "/home/ubuntu/StormData.txt";
    private boolean completed = false;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            fileReader = new FileReader(fileName);
            br = new BufferedReader(fileReader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.collector = collector;
        WordCountTopology.emitCounter = context.registerCounter("emitCounter");
        WordCountTopology.ackCounter = context.registerCounter("ackCounter");
    }

    public void nextTuple() {
        if (completed) {
            Utils.sleep(300000);
            return;
        }
        
        String line;
        try {
            if ((line = br.readLine()) != null) {
                LOG.debug("Emitting tuple: {}", line);
                this.collector.emit(new Values(line));
                WordCountTopology.emitCounter.inc();
            } else {
                System.out.print("complete!--------------------");
                completed = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void ack(Object id) {
    }

    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

}