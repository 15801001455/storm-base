package cn.crxy.storm_08;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

public class LocalTridentFilter {
	
	public static class PrintBolt extends BaseFunction{

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Integer value = tuple.getInteger(0);
			System.out.println(value);
		}
		
		
	}
	
	
	public static class Filter extends BaseFilter{

		@Override
		public boolean isKeep(TridentTuple tuple) {
			Integer value = tuple.getInteger(0);
			Integer flag = value%2;
			return flag==0?true:false;
		}
		
	}
	
	
	public static void main(String[] args) {
		FixedBatchSpout spout  = new FixedBatchSpout(new Fields("sentence"), 1, new Values(1),new Values(2),new Values(3));
		spout.setCycle(false);
		
		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("spout_id", spout)
		.each(new Fields("sentence"),new Filter())
		.each(new Fields("sentence"), new PrintBolt(), new Fields(""));
		
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("tridentTopology",new Config(), tridentTopology.build());
	}
	
	
	
	

}
