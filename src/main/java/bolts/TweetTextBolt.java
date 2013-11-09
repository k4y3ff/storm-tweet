package storm.starter.bolt;

import backtype.storm.topology.BasicOuputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class TweetTextBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple tuple, BasicOuputCollector collector) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer odf) {}

}