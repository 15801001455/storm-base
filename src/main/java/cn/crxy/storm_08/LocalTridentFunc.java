package cn.crxy.storm_08;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

public class LocalTridentFunc {
	
	public static class PrintBolt extends BaseFunction{

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Integer value = tuple.getInteger(0);
			System.out.println(value);
		}
		
		
	}
	
	
	public static void main(String[] args) {
		FixedBatchSpout spout  = new FixedBatchSpout(new Fields("sentence"), 1, new Values(1));
		spout.setCycle(false);
		
		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("spout_id", spout)
		.each(new Fields("sentence"), new PrintBolt(), new Fields(""));
		
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("tridentTopology",new Config(), tridentTopology.build());
	}
	
	
	
	

}
