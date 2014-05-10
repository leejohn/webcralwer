package jianli.ustc.paper;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

class MyAsyncCompleteHandler extends AsyncCompletionHandler<Response> {
	private static Logger logger = LoggerFactory.getLogger(MyAsyncCompleteHandler.class);
	protected AsyncHttpClient client;
	protected LinkedBlockingQueue<Response> pageQueue;
	protected LinkedBlockingQueue<String> linkQueue;
	protected String uri;
	protected BloomFilter<String> bloomFilter;
	protected Statistics stats;

	public MyAsyncCompleteHandler(BloomFilter<String> bloomFilter,
			LinkedBlockingQueue<String> linkQueue,
			LinkedBlockingQueue<Response> pageQueue,
			Statistics stats) {
		super();
		this.pageQueue = pageQueue;
		this.linkQueue = linkQueue;
		this.bloomFilter = bloomFilter;
		this.stats = stats;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	@Override
	public void onThrowable(Throwable t) {
		logger.error(t.getMessage());
		this.stats.failedRequest.incrementAndGet();
		if (!this.bloomFilter.mightContain(this.uri)) {
			try {
				this.linkQueue.put(this.uri);
				logger.error("Reput {}", this.uri);
			} catch (InterruptedException e) {

			}
		}
		
		 if (t instanceof TimeoutException) {
			 this.stats.timeoutFailure.incrementAndGet();
		 } else if (t instanceof ConnectException) {
			 this.stats.connectRefused.incrementAndGet();
		 } else {
			 this.stats.otherException.incrementAndGet();
		 }
	}

	@Override
	public Response onCompleted(Response response) throws Exception {
		String uri = response.getUri().toString();
		logger.info("Download {}", uri);
		if (this.bloomFilter.mightContain(uri)) {
			logger.warn("Download duplicated uri: {}", uri);
			return response;
		}
		this.bloomFilter.put(uri);
		this.pageQueue.put(response);
		this.stats.downloadedPage.incrementAndGet();
		this.stats.downloadedBytes.addAndGet(response.getResponseBodyAsBytes().length);
		return response;

	}

}

public class Downloader implements Runnable, Closeable {
	private static Logger logger = LoggerFactory.getLogger(Downloader.class);
	public boolean running = true;
	private final LinkedBlockingQueue<String> linkQueue;
	private final LinkedBlockingQueue<Response> pageQueue;
	private AsyncHttpClient client;
	private BloomFilter<String> bloomFilter;
	private Statistics stats;

	public Downloader(BloomFilter<String> bloomFilter, AsyncHttpClient client,
			LinkedBlockingQueue<String> linkQueue,
			LinkedBlockingQueue<Response> pageQueue,
			Statistics stats) {
		this.client = client;
		this.linkQueue = linkQueue;
		this.pageQueue = pageQueue;
		this.bloomFilter = bloomFilter;
		this.stats = stats;
	}

	public void run() {

		while (this.running) {

			try {
				String link = linkQueue.take();

				MyAsyncCompleteHandler handler = new MyAsyncCompleteHandler(
						this.bloomFilter, this.linkQueue, this.pageQueue, this.stats);
				handler.setUri(link);
				
//				logger.info("Send {}", link);
				this.stats.sendRequest.incrementAndGet();
				this.client.prepareGet(link).execute(handler);

			} catch (Exception e) {
				//logger.error(e.getMessage());
			}

		}

	}

	@Override
	public void close() throws IOException {
		this.running = false;
		
	}

}
