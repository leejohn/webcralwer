package jianli.ustc.paper;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

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

	public MyAsyncCompleteHandler(BloomFilter<String> bloomFilter,
			LinkedBlockingQueue<String> linkQueue,
			LinkedBlockingQueue<Response> pageQueue) {
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

		if (!this.bloomFilter.mightContain(this.uri)) {
			try {
				this.linkQueue.put(this.uri);
				logger.error("Reput {}", this.uri);
			} catch (InterruptedException e) {

			}
		}
		//
		// if (t instanceof IOException || t instanceof TimeoutException) {
		// System.err.println(t);
		// }
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

	public Downloader(BloomFilter<String> bloomFilter, AsyncHttpClient client,
			LinkedBlockingQueue<String> linkQueue,
			LinkedBlockingQueue<Response> pageQueue) {
		this.client = client;
		this.linkQueue = linkQueue;
		this.pageQueue = pageQueue;
		this.bloomFilter = bloomFilter;
	}

	public void run() {

		while (this.running) {

			try {
				String link = linkQueue.take();

				MyAsyncCompleteHandler handler = new MyAsyncCompleteHandler(
						this.bloomFilter, this.linkQueue, this.pageQueue);
				handler.setUri(link);
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
