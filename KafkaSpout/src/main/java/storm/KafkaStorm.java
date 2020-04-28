package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormTimer;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;
import java.util.UUID;

public class KafkaStorm {
    public static void main(String[] args) throws Exception{
        Config config = new Config();
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        String zkConnString = "localhost:2181";
        String BROKER_PORT="9092";
        String topic = "test";
        BrokerHosts hosts = new ZkHosts(zkConnString);
        Properties props = new Properties();
//default broker port = 9092
        props.put("metadata.broker.list", "localhost:" + BROKER_PORT);
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //config.put(Kafka, props);
        SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        //kafkaSpoutConfig.forceFromStart = true;
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        //KafkaSpout kafkaSpout=  new KafkaSpout(kafkaSpoutConfig);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout<String, String>(KafkaSpoutConfig.builder("localhost:9092", "testing").build()), 1);
//        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
        builder.setBolt("word-spitter", new SplitBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("word-counter", new CountBolt()).shuffleGrouping("word-spitter");

       // LocalCluster cluster = new LocalCluster();
       LocalCluster cluster = new LocalCluster("localhost",2181L);
        cluster.submitTopology("KafkaStormSample", config, builder.createTopology());

        Thread.sleep(5000);
        cluster.killTopology("KafkaStormSample");
        cluster.shutdown();
    }
}
