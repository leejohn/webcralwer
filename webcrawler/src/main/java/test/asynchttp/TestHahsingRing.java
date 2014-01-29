package test.asynchttp;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.apollo.util.HashRing;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;


public class TestHahsingRing {
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		
		HashRing<String, String> hashRing = new HashRing<String, String>();
		hashRing.add("192.168.1.100:2344");
		hashRing.add("192.168.1.101:6456");
		
		System.out.println(hashRing.get("http://www.qq.com"));
		System.out.println(hashRing.get("http://www.baidu.com"));
		
		hashRing.remove("192.168.1.101:6456");
		
		System.out.println(hashRing.get("http://www.qq.com"));
		System.out.println(hashRing.get("http://www.baidu.com"));
		//hashRing.add("sadfads");
		
		ZooKeeper zk = new ZooKeeper("localhost:2181", 3000, null);
		System.out.println(zk.exists("/", false));
		TimeUnit.SECONDS.sleep(1);
		int[] a = new int[]{123,5,45,46,7,56,78};
		System.out.println(zk.getChildren("/", null));
		zk.create("/asdf", "asdf".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

}
