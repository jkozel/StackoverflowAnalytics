package honza;

import java.util.Map;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MonthCounterBolt extends BaseRichBolt {
	private static final long serialVersionUID = -9215231212251371064L;

	private OutputCollector collector;
	private Map<String, Integer> countsByMonth;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.countsByMonth = new TreeMap<>();
	}

	@Override
	public void execute(Tuple input) {
		String monthStr = input.getStringByField(StackoverflowParserBolt.FIELD_CREATION_MONTH);
		if (monthStr != null) {
			Integer count = countsByMonth.get(monthStr);
			if (count == null) {
				count = 1;
			} else {
				count += 1;
			}
			countsByMonth.put(monthStr, count);
		}
		this.collector.ack(input);

		Boolean generateReports = input.getBooleanByField(StackoverflowParserBolt.FIELD_GENERATE_REPORTS);
		if (generateReports) {
			System.out.println("Total Monthly Counts = " + countsByMonth);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
