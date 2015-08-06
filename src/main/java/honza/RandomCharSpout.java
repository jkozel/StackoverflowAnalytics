package honza;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomCharSpout extends BaseRichSpout {
	private static final long serialVersionUID = 5112534763847841586L;

	private SpoutOutputCollector collector;
	private Random random;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.random = new Random();
	}

	@Override
	public void nextTuple() {
		Utils.sleep(500);
		short i = (short) random.nextInt(26);
		char c = (char) ('a' + (char) i);
		this.collector.emit(new Values(String.valueOf(c)));
		System.out.println("RandomCharSpout.nextInt(): emitted " + c);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tag"));
	}
}
