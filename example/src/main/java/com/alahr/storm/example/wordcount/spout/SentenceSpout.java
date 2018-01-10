package com.alahr.storm.example.wordcount.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SentenceSpout extends BaseRichSpout {
    private static Logger logger = LoggerFactory.getLogger(SentenceSpout.class);

    private SpoutOutputCollector collector;
    private int index = 0;

    private ConcurrentHashMap<String, Values> pending;

    private static String[] sentences = {
            "hello world",
            "hello storm",
            "this is the first example of storm",
            "welcome to the world of storm",
            "kafka includes producer and consumer",
            "topic partition broker",
            "today's topic is learning storm and kafka"
    };

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        this.collector = collector;
        this.pending = new ConcurrentHashMap<String, Values>();
    }

    public void nextTuple(){
        if(index >= this.sentences.length){
            return;
        }

        String messageId = "msg-"+index;
        logger.info("spout:{}", sentences[index]);
        Values vs = new Values(sentences[index]);
        this.pending.put(messageId, vs);
        this.collector.emit(vs, messageId);
        index++;
    }

    public void ack(Object msgId) {
        logger.info("ack messageId:\t"+msgId.toString());
        this.pending.remove(msgId);
    }

    public void fail(Object msgId) {
        logger.error("fail messageId:\t"+msgId.toString()+", then emit again");
        this.collector.emit(this.pending.get(msgId), msgId);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    public void close(){
    }
}
