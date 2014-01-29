package test.asynchttp;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeperMain;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

public class TestJsoup {
	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException, KeeperException {
		
		final ZooKeeper client = new ZooKeeper("localhost:2181", 3000, null);
		System.out.println( client.toString());
		for (String child : client.getChildren("/", false)) {
			System.out.println(child);
		}
		String data = new String(client.getData("/hello", false, null));
		client.exists("/hello", new Watcher() {
			
			public void process(WatchedEvent event) {
				try {
					System.out.println(event.getPath());
					System.out.println(event.getType());
					System.out.println(event.getState());
					System.out.println(new String(client.getData(event.getPath(), false, null)));
					
					client.exists("/hello", this);
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		});
		
		client.getChildren("/spider-cluster", new Watcher() {
			
			public void process(WatchedEvent event) {
				try {
					System.out.println(event.getPath());
					System.out.println(event.getType());
					System.out.println(event.getState());
//					System.out.println(new String(client.getData(event.getPath(), false, null)));
					
					for (String child : client.getChildren(event.getPath(), this)) {
						System.out.println(child);
					}
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
		});
		System.out.println(data);
		TimeUnit.MINUTES.sleep(100);
		//client.close();
//		AsyncHttpClient client = new AsyncHttpClient();
//		Response response = client.prepareGet("http://www.qq.com").execute().get();
//		Document doc = Jsoup.parse(response.getResponseBody(), response.getUri().toString());
//		
//		org.jsoup.select.Elements elements = doc.select("a[href]");
//		for (Element e : elements) {
//			System.out.println(e.attr("abs:href"));
//		}
//		client.close();
		
//		BloomFilter<String> bloomFilter = BloomFilter.create(new Funnel<String>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			public void funnel(String from, PrimitiveSink into) {
//				into.putString(from, Charsets.US_ASCII);
//				
//			}
//		}, 500000);
//		bloomFilter.put("http://dsfadsfaegrdasgdfrg.com");
//		
//		System.out.println(bloomFilter.mightContain("http://dsfadsfaegrdasgdfrg.com"));
//		System.out.println(bloomFilter.mightContain("fdgreg"));
	}
}
