package cn.crxy.storm_08;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

public class ClusterStormDrpcClient {
	
	public static void main(String[] args) {
		DRPCClient drpcClient = new DRPCClient("192.168.1.170", 3772);
		try {
			String result = drpcClient.execute("hello", "aaaaa");
			
			System.out.println(result);
		} catch (TException e) {
			e.printStackTrace();
		} catch (DRPCExecutionException e) {
			e.printStackTrace();
		}
		
	}

}
