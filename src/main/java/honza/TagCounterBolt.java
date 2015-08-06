package honza;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TagCounterBolt extends BaseRichBolt {
	private static final long serialVersionUID = -5656970455833881406L;

	private OutputCollector collector;
	private Map<String, Map<String, Integer>> countsByMonthAndTag;
	private Map<String, Integer> totalCountsByTag;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.countsByMonthAndTag = new TreeMap<>();
		this.totalCountsByTag = new TreeMap<>();
	}

	@Override
	public void execute(Tuple input) {
		List<String> tags = (List<String>) input.getValueByField(StackoverflowParserBolt.FIELD_TAGS);
		String month = input.getStringByField(StackoverflowParserBolt.FIELD_CREATION_MONTH);
		if (tags != null) {
			for (String tag : tags) {
				Map<String, Integer> countsByMonth = countsByMonthAndTag.get(tag);
				if (countsByMonth == null) {
					countsByMonth = new TreeMap<>();
					countsByMonthAndTag.put(tag, countsByMonth);
				}
				Integer count = countsByMonth.get(month);
				count = count != null ? count + 1 : 1;
				countsByMonth.put(month, count);

				Integer totalCount = totalCountsByTag.get(tag);
				totalCount = totalCount != null ? totalCount + 1 : 1;
				totalCountsByTag.put(tag, totalCount);
			}
		}
		this.collector.ack(input);

		Boolean generateReports = input.getBooleanByField(StackoverflowParserBolt.FIELD_GENERATE_REPORTS);
		if (generateReports) {
			System.out.println("Total storm = " + totalCountsByTag.get("storm"));
			System.out.println("Monthly storm = " + countsByMonthAndTag.get("storm"));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
