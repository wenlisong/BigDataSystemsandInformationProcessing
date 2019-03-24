package myapp;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 * WordCountBolt
 */
public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private LinkedHashMap<String, Integer> counts;
    private static final int TARGET_FAIL_COUNT = 10;
    private int failCounter = 0;
    public static final String OUTPUT_FILE_PATH = "/home/ubuntu/output-word-count.txt";

    //sort hash map by value
    static <K, V> void sortByValue(LinkedHashMap<K, V> m, final Comparator<? super V> c) {
        List<Map.Entry<K, V>> entries = new ArrayList<Map.Entry<K, V>>(m.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> lhs, Map.Entry<K, V> rhs) {
                return c.compare(lhs.getValue(), rhs.getValue());
            }
        });

        m.clear();
        for(Map.Entry<K, V> e : entries) {
            m.put(e.getKey(), e.getValue());
        }
    }
     
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new LinkedHashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");

        if (word.equals("the") && failCounter < TARGET_FAIL_COUNT) {
            this.collector.fail(input);
            failCounter += 1;
            return ;
        }
        if (counts.containsKey(word)) {
            counts.put(word, counts.get(word) + 1);
        } else {
            counts.put(word, 1);
        }
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup() {
        sortByValue(counts, new Comparator<Integer>() {
            @Override
            public int compare(Integer i1, Integer i2) {
                return i2.compareTo(i1);
            }
        });

        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(OUTPUT_FILE_PATH);
            bw = new BufferedWriter(fw);
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                bw.write(entry.getKey() + "\t" + String.valueOf(entry.getValue()) + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                }
                if (fw != null) {
                    fw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}