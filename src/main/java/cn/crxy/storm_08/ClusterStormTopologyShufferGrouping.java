package cn.crxy.storm_08;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 数据累加的操作
 * @author Administrator
 *
 */
public class ClusterStormTopologyShufferGrouping {
	public static class DataSourceSpout extends BaseRichSpout{
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;
		
		
		/**
		 * 本实例运行的是被调用一次，只能执行一次。
		 */
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}
		/**
		 * 死循环的调用，心跳
		 */
		int i=0;
		public void nextTuple() {
			System.out.println("spout:"+i);
			this.collector.emit(new Values(i++));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		/**
		 * 声明输出的内容
		 */
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("num"));
		}
	}
	
	
	
	public static class Sumbolt extends BaseRichBolt{
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}
		int sum = 0;
		public void execute(Tuple input) {
			//input.getInteger(0);
			Integer value = input.getIntegerByField("num");
			sum+=value;
			System.out.println("当前线程ID"+Thread.currentThread().getId()+"---value:"+value);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}
	}
	
	
	
	
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("spout_id", new DataSourceSpout());
		//spout流到bolt的数据bolt启动三个线程(executor)进行接收,shuffleGrouping意思就是给三个bolt线程随机负载均衡发送 shuffleGrouping也是最常用的spout发送数据到bolt的分组方式
		topologyBuilder.setBolt("bolt_id", new Sumbolt(),3).shuffleGrouping("spout_id");
		
		String simpleName = ClusterStormTopologyShufferGrouping.class.getSimpleName();
		try {
			StormSubmitter.submitTopology(simpleName, new Config(), topologyBuilder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
		
	}
	
	
	
	
	

}
