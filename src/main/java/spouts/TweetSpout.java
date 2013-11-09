package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

// NEED TO INCLUDE TWITTER HOSEBIRD IMPORTS
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TweetSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;

	// HOSEBIRD CLIENT STUFF
	BasicClient client;
	BlockingQueue<String> queue;

	// HOSEBIRD AUTHENTICATION STUFF
	// NEED TO MOVE TO SEPARATE FILE
	String consumerKey = "HcDXDSZqLrxt3JMAxZihw";
	String consumerSecret = "EYg4aihHbWfjYlWLYC3JZquYhkitp7MTYsbue5PE";
	String token = "7178502-zUZgLNHbxag3qHWbCwPf7bc6EbnC74VWDxBBuCCGsi";
	String secret = "UNq1P387drzu8ZUjvpJnY33gbE2a1MykbszdpS2Qg";

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;

		queue = new LinkedBlockingQueue<String>(10000);
		StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint(); // WHY SAMPLE?
		endpoint.stallWarnings(false);
		Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

		client = new ClientBuilder()
			.name("sampleExampleClient")
			.hosts(Constants.STREAM_HOST)
			.endpoint(endpoint)
			.authentication(auth)
			.processor(new StringDelimitedProcessor(queue))
			.build();

		client.connect();
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		// String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
		// 	"four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
		// String sentence = sentences[_rand.nextInt(sentences.length)];

		String msg = queue.poll();

		_collector.emit(new Values(msg));
	}

	@Override
	public void ack(Object id) {}

	@Override
	public void fail(Object id) {
		// client.stop(); // WHERE SHOULD I PUT THIS?
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}