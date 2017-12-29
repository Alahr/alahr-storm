package com.alahr.storm.example.wordcount.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Logger logger = LoggerFactory.getLogger(SplitSentenceBolt.class);

    public SplitSentenceBolt() {
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for(String word : words){
            word = word.trim();
            if(null != word && !word.equals("")){
                word = word.toLowerCase();
                collector.emit(new Values(word));
            }
        }
        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    public void cleanup() {
    }
}
