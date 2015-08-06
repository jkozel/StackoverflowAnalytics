package honza;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Parses a Stackoverflow entry into a month and a list of tags for each post.
 *
 * @author jkozel
 * @see StackoverflowParser
 */
public class StackoverflowParserBolt extends BaseRichBolt {
	private static final long serialVersionUID = 584995292805474631L;
	private static final Logger logger = LoggerFactory.getLogger(StackoverflowParserBolt.class);

	public static final String FIELD_GENERATE_REPORTS = "generateReports";
	public static final String FIELD_CREATION_MONTH = "creationMonth";
	public static final String FIELD_TAGS = "tags";

	private OutputCollector collector;
	private StackoverflowParser parser;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		parser = new StackoverflowParser();
	}

	@Override
	public void execute(Tuple input) {
		String rawStr = input.getStringByField(FileReaderSpout.FIELD_LINE);
		String creationMonthStr = null;
		List<String> tags = null;
		if (rawStr != null && rawStr.length() > 0) {
			try {
				creationMonthStr = parser.extractCreationMonthStr(rawStr);
			} catch (IllegalArgumentException e) {
				logger.error("No CreationDate found", e);
			}
			try {
				tags = parser.extractTags(rawStr);
			} catch (IllegalArgumentException e) {
				// Missing tags are a normal condition. Continue processing, but without any tags.
			}
		}
		
		boolean generateReports = input.getBooleanByField(FileReaderSpout.FIELD_GENERATE_REPORTS);
		this.collector.emit(new Values(generateReports, creationMonthStr, tags));
		this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FIELD_GENERATE_REPORTS, FIELD_CREATION_MONTH, FIELD_TAGS));
	}
}
