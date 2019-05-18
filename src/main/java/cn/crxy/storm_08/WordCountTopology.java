package cn.crxy.storm_08;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
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
 * 单词计数
 * @author Administrator
 *
 */
public class WordCountTopology {
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
		public void nextTuple() {
			//读取指定目录下所有文件
			Collection<File> files = FileUtils.listFiles(new File("d:\\test"), new String[]{"txt"}, true);
			for (File file : files) {
				try {
					//获取每个文件的所有数据
					List<String> lines = FileUtils.readLines(file);
					//把每一行数据发射出去
					for (String line : lines) {
						this.collector.emit(new Values(line));
					}
					FileUtils.moveFile(file, new File(file.getAbsolutePath()+System.currentTimeMillis()));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		/**
		 * 声明输出的内容
		 */
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("line"));
		}
	}
	
	
	public static class Splitbolt extends BaseRichBolt{
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}
		
		
		public void execute(Tuple input) {
			//获取每一行数据
			String line= input.getStringByField("line");
			//把数据切分成一个个的单词
			String[] words = line.split("\t");
			for (String word : words) {
				//把每个单词都发射数据
				this.collector.emit(new Values(word));
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("words"));
		}
	}
	
	
	public static class Countbolt extends BaseRichBolt{
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}
		
		HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
		public void execute(Tuple input) {
			//获取每一个单词
			String word = input.getStringByField("words");
			//对所有的单词进行汇总
			Integer value = hashMap.get(word);
			if(value==null){
				value = 0;
			}
			value++;
			hashMap.put(word, value);
			
			//把结果打印出来
			System.out.println("==================================");
			for (Entry<String, Integer> entry : hashMap.entrySet()) {
				System.out.println(entry);
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}
	}
	
	
	
	
	
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("spout_id", new DataSourceSpout());
		topologyBuilder.setBolt("bolt_id", new Splitbolt()).shuffleGrouping("spout_id");
		topologyBuilder.setBolt("bolt_id_2", new Countbolt()).shuffleGrouping("bolt_id");
		
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("topology", new Config(), topologyBuilder.createTopology());
	}
	
	
	
	
	

}
