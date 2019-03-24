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
import org.apache.storm.utils.Utils;

/**
 * WordCountBolt
 */
public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private LinkedHashMap<String, Integer> counterMap;
    private static final int TARGET_FAIL_COUNT = 10;
    private int failCounter = 0;
    public static final String RESULT_PATH = "/home/yongbiaoai/remote/wordcount_result_swl.txt";

    static <K, V> void orderByValue(LinkedHashMap<K, V> m, final Comparator<? super V> c) {
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
        this.counterMap = new LinkedHashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");

        if (word.equals("the") && failCounter < TARGET_FAIL_COUNT) {
            this.collector.fail(input);
            failCounter += 1;
            return ;
        }

        if (counterMap.containsKey(word)) {
            counterMap.put(word, counterMap.get(word) + 1);
        } else {
            counterMap.put(word, 1);
        }
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup() {
        System.out.println("cleanup, sortByValue counterMap start");
        // sort and save result into local file
        orderByValue(counterMap, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2.compareTo(o1);
            }
        });

        System.out.println("cleanup, start to save counterMap into file");
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(RESULT_PATH);
            writer = new BufferedWriter(fw);
            for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
                writer.write(entry.getKey() + "\t" + String.valueOf(entry.getValue()));
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
                if (fw != null) {
                    fw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("cleanup, end save counterMap into file");
    }
}