package test.asynchttp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.AsyncHttpClientConfig.Builder;
import com.ning.http.client.Response;
/**
 * Hello world!
 *
 */
public class App 
{
	public static class MyAsyncCompleteHandler extends
			AsyncCompletionHandler<Response> {
		
		protected AsyncHttpClient client;
		protected LinkedBlockingQueue<Response> pageQueue;
		protected LinkedBlockingQueue<String> linkQueue;
		protected String uri;
		protected BloomFilter<String> bloomFilter;
		
		public MyAsyncCompleteHandler(BloomFilter<String> bloomFilter, LinkedBlockingQueue<String> linkQueue, LinkedBlockingQueue<Response> pageQueue) {
			super();
			this.pageQueue = pageQueue;
			this.linkQueue = linkQueue;
			this.bloomFilter = bloomFilter;
		}
		
		public void setUri(String uri) { 
			this.uri = uri;
		}
		
		
		@Override
		public void onThrowable(Throwable t) {
			System.err.println(t);
			
			if (! this.bloomFilter.mightContain(this.uri)) {
				try {
					this.linkQueue.put(this.uri);
					System.err.println("Reput " + this.uri);
				} catch (InterruptedException e) {
					
				}
			}
//			
//			if (t instanceof IOException || t instanceof TimeoutException) {
//				System.err.println(t);
//			}
		}
		
		@Override
		public Response onCompleted(Response response) throws Exception {
			String uri = response.getUri().toString();
			System.out.println(uri);
			if (this.bloomFilter.mightContain(uri)) {
				System.err.println("Download duplicated uri: " + uri);
				return response;
			}
			this.bloomFilter.put(uri);
			this.pageQueue.put(response);

			return response;

		}

	}
	
	public static class Downloader implements Runnable {
		
		public boolean running = true;
		private final LinkedBlockingQueue<String> linkQueue;
		private final LinkedBlockingQueue<Response> pageQueue;
		private AsyncHttpClient client;
		private BloomFilter<String> bloomFilter;
		
		public Downloader(BloomFilter<String> bloomFilter, AsyncHttpClient client, LinkedBlockingQueue<String> linkQueue, LinkedBlockingQueue<Response> pageQueue) {
			this.client = client;
			this.linkQueue = linkQueue;
			this.pageQueue = pageQueue;
			this.bloomFilter = bloomFilter;
		}
		
		public void run() {

			while (this.running) {

				try {					
					String link = linkQueue.take();
					
					MyAsyncCompleteHandler handler = new MyAsyncCompleteHandler(this.bloomFilter, this.linkQueue, this.pageQueue);
					handler.setUri(link);
					this.client.prepareGet(link).execute(handler);
					
				} catch (Exception e) {
					System.err.println(e);
				}

			}
			
		}
		
	}
	
	public static class LinkExtractor implements Runnable {

		public boolean running = true;
		private final LinkedBlockingQueue<String> linkQueue;
		private final LinkedBlockingQueue<Response> pageQueue;
		private final BloomFilter<String> bloomFilter;
		
		public LinkExtractor(BloomFilter<String> bloomFilter, LinkedBlockingQueue<String> linkQueue, LinkedBlockingQueue<Response> pageQueue) {
			this.linkQueue = linkQueue;
			this.pageQueue = pageQueue;
			this.bloomFilter = bloomFilter;
		}
		
		public void run() {
			
			HashSet<String> urlSet = new HashSet<String>();
			
			while (this.running) {
				
				try {
					Response response = pageQueue.take();
					
					Document doc = Jsoup.parse(response.getResponseBody(), response.getUri().toString());
					Elements links = doc.select("a[href]");
					
					for (Element linkElement : links) {
						String link = linkElement.attr("abs:href");
						if (this.bloomFilter.mightContain(link)) {
							System.err.println("Link already downloaded: " + link);
							continue;
						}
						
						
						
						if  (link == "" || link.endsWith(".exe") || link.endsWith(".jpg") || link.endsWith(".png")) {
//							System.out.println("Ignore link " + link);
							continue;
						}

						boolean notContain = urlSet.add(link);
						if (notContain) {
							this.linkQueue.put(link);
						}
					}
					
					urlSet.clear();
//						
				} catch (InterruptedException e) {
					System.err.println(e);
				} catch (MalformedURLException e) {
					System.err.println(e);;
				} catch (IOException e) {
					System.err.println(e);;
				}
			}
			
		}
		
	}
	
    public static void main( String[] args ) throws Exception
    {
    	BloomFilter<String> bloomFilter = BloomFilter.create(new Funnel<String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void funnel(String from, PrimitiveSink into) {
				into.putString(from, Charsets.US_ASCII);
				
			}
		}, 500000);
    	
    	LinkedBlockingQueue<String> linkQueue = new LinkedBlockingQueue<String>();
//    	
//    	linkQueue.put("http://www.sohu.com");
//    	linkQueue.put("http://www.hao123.com");
//    	
//    	linkQueue.put("http://www.qq.com");
//    	linkQueue.put("http://www.qq.com");
//    	linkQueue.put("http://www.qq.com");
//    	linkQueue.put("http://www.qq.com");
//    	linkQueue.put("http://www.qq.com");
//    	linkQueue.put("http://www.qq.com");
//    	linkQueue.put("http://www.qq.com");
    	linkQueue.put("http://www.qq.com");

    	
    	LinkedBlockingQueue<Response> pageQueue = new LinkedBlockingQueue<Response>();
    	
        Builder builder = new AsyncHttpClientConfig.Builder();
        builder.setMaximumNumberOfRedirects(20).setFollowRedirects(true);
        builder.setMaximumConnectionsTotal(1024 * 4);
        builder.setRequestTimeoutInMs(10000);
        builder.setMaxConnectionLifeTimeInMs(20000);
        builder.setUserAgent("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)");
        builder.setMaximumConnectionsPerHost(100);
        builder.setMaxRequestRetry(0);
    	final AsyncHttpClient asyncHttpClient = new AsyncHttpClient(builder.build());
    	
    	final ExecutorService executor = Executors.newCachedThreadPool();
    	executor.execute(new Downloader(bloomFilter, asyncHttpClient, linkQueue, pageQueue));
    	executor.execute(new LinkExtractor(bloomFilter, linkQueue, pageQueue));
//    	executor.execute(new LinkExtractor(bloomFilter, linkQueue, pageQueue));
    	
    	BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    	String input;
    	Executors.newSingleThreadScheduledExecutor().schedule(new Runnable() {
			
			public void run() {
				System.out.println("Closing.........");
				asyncHttpClient.closeAsynchronously();
				executor.shutdownNow();
				
			}
		}, 5, TimeUnit.MINUTES);
    	
    	while ((input = reader.readLine()) != null) {
    		
    		if (input.equals("link")) {
    			System.out.println("Link length: " + linkQueue.size());
    			
    		} else if (input.equals("page")) {
    			System.out.println("Page lenght: " + pageQueue.size());
    			
    		}
    	}

    }
    
    
}
