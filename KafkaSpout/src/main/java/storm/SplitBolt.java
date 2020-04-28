package storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt implements IRichBolt {
    private OutputCollector collector;


    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        System.out.println("gggggggggggghdfgdkjsfdds");
    }

    public void execute(Tuple input) {
        String sentence = input.getString(1);
        String[] words = sentence.split(" ");
        System.out.println("XXXXXXXXXXXXXXXXXXXXX");
        for(String word: words) {
            word = word.trim();
            if(! word.isEmpty()) {
                word = word.toLowerCase();
                collector.emit(new Values(word));
            }

        }

        collector.ack(input);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }


    public void cleanup() {}


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
