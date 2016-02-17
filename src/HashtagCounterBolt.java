import java.util.*;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * Created by derek_000 on 16/02/16.
 */
public class HashtagCounterBolt implements IRichBolt {
    Map<String, Integer> counterMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.counterMap = new HashMap<String, Integer>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String key = tuple.getString(0);

        if(!counterMap.containsKey(key)){
            counterMap.put(key, 1);
        }else{
            Integer c = counterMap.get(key) + 1;
            counterMap.put(key, c);
        }

        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        SortedSet<Map.Entry<String,Integer>> sorted = entriesSortedByValues(counterMap);
        for( Map.Entry<String,Integer> entry :  sorted){
            System.out.println("Result: " + entry.getKey()+" : " + entry.getValue());
        }
    }

    private SortedSet<Map.Entry<String,Integer>> entriesSortedByValues(Map<String, Integer> map) {
        SortedSet<Map.Entry<String,Integer>> sortedEntries = new TreeSet<>(
                (e1, e2) -> {
                    int res = e1.getValue().compareTo(e2.getValue());
                    return res != 0 ? res : 1;
                }
        );
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}