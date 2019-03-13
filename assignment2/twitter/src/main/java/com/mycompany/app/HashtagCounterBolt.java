package com.mycompany.app;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class HashtagCounterBolt implements IRichBolt {
   private static final long serialVersionUID = 1L;
   Map<String, Integer> counterMap;
   private OutputCollector collector;

   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.counterMap = new HashMap<String, Integer>();
      this.collector = collector;
   }

   public void execute(Tuple tuple) {
      try {
         if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
               && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
                  List<String> keys = new ArrayList<String>();
                  keys.addAll(this.counterMap.keySet());
                  try {
                      PrintWriter out = new PrintWriter("/home/ubuntu/twitter_stream.txt");
                      for (String key : keys) {
                          out.println(key + "\t" + this.counterMap.get(key));
                      }
                      out.close();
                  } catch (Exception e) {
          
                  }
         }
      
         String key = tuple.getString(0);
         if(!counterMap.containsKey(key)){
            counterMap.put(key, 1);
         } else {
            Integer c = counterMap.get(key) + 1;
            counterMap.put(key, c);
         }
         collector.ack(tuple);
      } catch (Exception e) {
         //TODO: handle exception
      }
   }

   public void cleanup() {
      for(Map.Entry<String, Integer> entry:counterMap.entrySet()){
         System.out.println("Result: " + entry.getKey()+" : " + entry.getValue());
      }
   }

   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("hashtag"));
   }
	
   public Map<String, Object> getComponentConfiguration() {
      Config conf = new Config();
      conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 600);
      return conf;
   }
	
}