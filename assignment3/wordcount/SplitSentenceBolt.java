package myapps;

import java.util.Map;


/**
 * SplitSentenceBolt
 */
public class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        
        for (String word : words) {
            word = word.trim();

            if (!word.isEmpty()) {
                word = word.toLowerCase();
                collector.emit(new Values(word));
            }

        }
        
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    
}