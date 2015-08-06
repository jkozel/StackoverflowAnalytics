package honza;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.Broker;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.GlobalPartitionInformation;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Topology to analyse Stackoverflow posts by month, by tag, and by month and tag.
 *
 * @author jkozel
 */
public class StackoverflowTopology {
	private static final Logger logger = LoggerFactory.getLogger(StackoverflowTopology.class);

	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();

		GlobalPartitionInformation partInfo = new GlobalPartitionInformation();
		partInfo.addPartition(0, new Broker("localhost"));
		BrokerHosts hosts = new StaticHosts(partInfo);
		hosts = new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "test", "/honza/kafkaStorm", "kafkaOffsets");
////        List<String> zkServers = _spoutConfig.zkServers;
////        if (zkServers == null) {
////            zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
////        }
////        Integer zkPort = _spoutConfig.zkPort;
////        if (zkPort == null) {
////            zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
////        }
		builder.setSpout("spout", new KafkaSpout(spoutConfig));

//		builder.setSpout("spout", new FileReaderSpout());
		builder.setBolt("stackoverflowParserBolt", new StackoverflowParserBolt())
			.shuffleGrouping("spout");

		builder.setBolt("monthCounterBolt", new MonthCounterBolt())
			.fieldsGrouping("stackoverflowParserBolt", new Fields(StackoverflowParserBolt.FIELD_CREATION_MONTH));

		builder.setBolt("tagCounterBolt", new TagCounterBolt())
			.shuffleGrouping("stackoverflowParserBolt");

		StormTopology topology = builder.createTopology();

		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(3);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("StackoverflowTopology", conf, topology);
//		Thread.sleep(100000);
//		System.out.println("Shutting down topology after sleeping for a suitable time.");
//		cluster.shutdown();
	}
}
