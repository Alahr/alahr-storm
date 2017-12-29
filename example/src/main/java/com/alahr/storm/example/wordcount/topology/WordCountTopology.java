package com.alahr.storm.example.wordcount.topology;

import com.alahr.storm.example.wordcount.bolt.SplitSentenceBolt;
import com.alahr.storm.example.wordcount.bolt.WordCountBolt;
import com.alahr.storm.example.wordcount.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountTopology {
    private static Logger logger = LoggerFactory.getLogger(WordCountTopology.class);

    public static void main(String[] args){

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sentence-spout", new SentenceSpout(), 1);
        builder.setBolt("split-bolt", new SplitSentenceBolt(), 2).localOrShuffleGrouping("sentence-spout");
        builder.setBolt("count-bolt", new WordCountBolt()).fieldsGrouping("split-bolt", new Fields("word"));

        Config conf = new Config();

        conf.setDebug(true);

        if (args != null && args.length > 0) {
            logger.info("Cluster topology: {}", args[0]);
            try{
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }
            catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e){
                logger.error("cluster submitTopology", e);
            }
        }
        else {
            String topName = "example-word-count";
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topName, conf, builder.createTopology());

            Utils.sleep(5000);
            cluster.killTopology(topName);
            cluster.shutdown();
        }
    }
}
