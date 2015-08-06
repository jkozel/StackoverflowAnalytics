package honza;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reads data from a text file.
 * Each line is emitted as a tuple.
 * local mode
 */
public class FileReaderSpout extends BaseRichSpout {
	private static final long serialVersionUID = -7663523169773141243L;
	private static final Logger logger = LoggerFactory.getLogger(FileReaderSpout.class);

	private static final long MAX_LINES = Long.MAX_VALUE;
	public static final String FIELD_GENERATE_REPORTS = "generateReports";
	public static final String FIELD_LINE = "line";

	private String fileName = "/Users/jkozel/Downloads/Posts1m-tail.xml";
	private SpoutOutputCollector collector;
	private BufferedReader reader;
	private AtomicLong linesRead;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		linesRead = new AtomicLong(0);
		this.collector = collector;
		try {
			// TODO: Read the file name from config
//			fileName = (String) conf.get("fileReaderSpout.file");
			reader = new BufferedReader(new FileReader(fileName));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void deactivate() {
		try {
			reader.close();
		} catch (IOException e) {
			logger.warn("Problem closing file", e);
		}
	}

	@Override
	public void nextTuple() {
		try {
			String line = reader.readLine();
			long id = linesRead.incrementAndGet();
			if (line != null && MAX_LINES > id) {
				this.collector.emit(new Values(false, line), id);
				if (id % 5000 == 0) {
					System.out.println("Read line " + id);
				}
			} else {
				this.collector.emit(new Values(true, ""), id);
				System.out.println("Finished reading file, " + linesRead.get() + " lines read");
				Thread.sleep(100000);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Failed in nextTuple()", e);
		}
	}

	@Override
	public void fail(Object id) {
		logger.error("Failed line number " + id);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FIELD_GENERATE_REPORTS, FIELD_LINE));
	}
}