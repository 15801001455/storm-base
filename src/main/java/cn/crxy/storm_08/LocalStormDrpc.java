package cn.crxy.storm_08;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LocalStormDrpc {
	
	public static class MyBolt extends BaseRichBolt{
		private Map stormConf; 
		private TopologyContext context;
		private OutputCollector collector;
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}
		/**
		 * tuple中会传递过来连个参数
		 * 第一个表示是请求的ID，第二个表示是请求的参数
		 */
		public void execute(Tuple input) {
			String value = input.getString(1);
			value = "hello1 "+value;
			
			this.collector.emit(new Values(input.getValue(0),value));
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id","value"));
		}
	}
	
	
	public static void main(String[] args) {
		LinearDRPCTopologyBuilder linearDRPCTopologyBuilder = new LinearDRPCTopologyBuilder("hello");
		linearDRPCTopologyBuilder.addBolt(new MyBolt());
		
		LocalCluster localCluster = new LocalCluster();
		LocalDRPC drpc = new LocalDRPC();
		localCluster.submitTopology("drpc",new Config(), linearDRPCTopologyBuilder.createLocalTopology(drpc));
		
		
		
		String result = drpc.execute("hello", "storm");
		System.err.println("客户端调用结果："+result);
		
		
		
	}
	
	
	

}
