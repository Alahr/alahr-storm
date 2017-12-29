package com.alahr.storm.example.wordcount.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class WordCountBolt extends BaseRichBolt {

    private Logger logger = LoggerFactory.getLogger(WordCountBolt.class);

    private OutputCollector collector;

    private Map<String, Integer> counts;

    public WordCountBolt() {
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Integer count = this.counts.get(word);
        if(count == null){
            count = 0;
        }
        count++;
        this.counts.put(word, count);
        logger.info("{}:{}", word, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void cleanup() {
    }
}
