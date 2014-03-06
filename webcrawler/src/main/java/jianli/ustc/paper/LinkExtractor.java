package jianli.ustc.paper;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;
import com.ning.http.client.Response;

public class LinkExtractor implements Runnable, Closeable {
	private static Logger logger = LoggerFactory.getLogger(LinkExtractor.class);
	private boolean running = true;
	private LinkedBlockingQueue<String> linkQueue;
	private LinkedBlockingQueue<Response> pageQueue;
	private BloomFilter<String> bloomFilter;
	private	HashRingFromZK hashRing;
	private UrlSenders urlSenders;
	
	public LinkExtractor(UrlSenders urlSenders, HashRingFromZK hashRing, BloomFilter<String> bloomFilter,
			LinkedBlockingQueue<String> linkQueue,
			LinkedBlockingQueue<Response> pageQueue) {
		this.linkQueue = linkQueue;
		this.pageQueue = pageQueue;
		this.bloomFilter = bloomFilter;
		this.hashRing = hashRing;
		this.urlSenders = urlSenders;
	}

	public void run() {

		HashSet<String> urlSet = new HashSet<String>();

		while (this.running) {

			try {
				Response response = pageQueue.take();

				Document doc = Jsoup.parse(response.getResponseBody(), response
						.getUri().toString());
				Elements links = doc.select("a[href]");

				for (Element linkElement : links) {
					String link = linkElement.attr("abs:href");
					if (this.bloomFilter.mightContain(link)) {
						logger.warn("Link already downloaded: {}", link);
						continue;
					}

					if (link == "" || link.endsWith(".exe")
							|| link.endsWith(".jpg") || link.endsWith(".png")) {
						// System.out.println("Ignore link " + link);
						continue;
					}

					urlSet.add(link);
					
				}
				logger.info("Url given to urlsender {}", urlSet.size());
				urlSenders.send(urlSet);
				urlSet.clear();
				
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
			} catch (MalformedURLException e) {
				logger.error(e.getMessage());
				
			} catch (IOException e) {
				logger.error(e.getMessage());
				
			}
		}

	}

	@Override
	public void close() throws IOException {
		this.running = false;
	}
}
