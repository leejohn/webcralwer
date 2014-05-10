package jianli.ustc.paper;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Priority;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.AsyncHttpClientConfig.Builder;
import com.ning.http.client.Response;

public class Crawler {
	public static void main(String[] args) throws InterruptedException, RemoteException, SocketException, KeeperException {
		ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%p] %c{1} %d{HH:mm:ss} %m%n"));
		appender.setThreshold(Priority.INFO);
		BasicConfigurator.configure(appender);
		
		final Logger logger = LoggerFactory.getLogger("Main");
		final long startTime = System.currentTimeMillis();
    	String ipAddress = getNetWorkInterface();
    	if (ipAddress == null) {
    		throw new RuntimeException("Couldn't not find a valid ip address");
    	}
		
		BloomFilter<String> bloomFilter = BloomFilter.create(new Funnel<String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void funnel(String from, PrimitiveSink into) {
				into.putString(from, Charsets.US_ASCII);
				
			}
		}, 500000);
    	
		//Create a statistics instance for counting
		final Statistics stats = new Statistics();
		
    	LinkedBlockingQueue<String> linkQueue = new LinkedBlockingQueue<String>();
    	LinkedBlockingQueue<Response> pageQueue = new LinkedBlockingQueue<Response>();
    	final ExecutorService executorService = Executors.newCachedThreadPool();

    	linkQueue.put("http://www.hao123.com");
    	
    	System.setProperty("java.rmi.server.hostname", ipAddress);
		System.setProperty("java.rmi.server.useCodebaseOnly", "false");
		System.setProperty("java.security.policy", "file:///home/jianli/git/ustcpaper/webcrawler/src/main/java/all.policy");

    	
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}
    	
    	UrlSender urlSender = new UrlSenderImpl(linkQueue);
    	UrlSender remoteUrlSender = (UrlSender) UnicastRemoteObject.exportObject(urlSender, 0);
    	
    	int registryPort = 1099;
    	Registry registry = LocateRegistry.createRegistry(registryPort);
    	registry.rebind("crawler", remoteUrlSender);
    	/**
    	 * try connect to zookeeper with the local ip address, and create /spider-cluster if that doesn't exist
    	 * then create a child under /spider-cluster with name as ip:port.
    	 * ip is the local connecting ip address, port is the rmiregistry port,
    	 * the ip address will be passed to UrlSenders for quickly identifying one node is acatually himself.
    	 */   	    	
    	
    	String path = "/spider-cluster";
    	String zookeeperPort = "2181";
    	
    	String selfHostPort = String.format("%s:%s", ipAddress, registryPort);
    	final HashRingFromZK hashRing = new HashRingFromZK(selfHostPort, 
    			"localhost:" + zookeeperPort, 3000, null, false, path);
    	
    	final String nodeName = String.format("/spider-cluster/%s:%s", ipAddress, registryPort);
    	hashRing.Initialize(nodeName);
    	
    	UrlSenders urlSenders = new UrlSenders(executorService, hashRing, linkQueue, "self", ipAddress , String.valueOf(registryPort), "crawler");
    	
        Builder builder = new AsyncHttpClientConfig.Builder();
        builder.setMaximumNumberOfRedirects(20).setFollowRedirects(true);
        builder.setMaximumConnectionsTotal(1000);
        builder.setRequestTimeoutInMs(10000);
        builder.setMaxConnectionLifeTimeInMs(20000);
        builder.setUserAgent("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)");
        builder.setMaximumConnectionsPerHost(100);
        builder.setMaxRequestRetry(3);
    	final AsyncHttpClient asyncHttpClient = new AsyncHttpClient(builder.build());
    	final Downloader downloader = new Downloader(bloomFilter, asyncHttpClient, linkQueue, pageQueue, stats);
    	final LinkExtractor linkExtracator = new LinkExtractor(urlSenders, hashRing, bloomFilter, linkQueue, pageQueue);
    	executorService.execute(downloader);
    	executorService.execute(linkExtracator);
    	
    	Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			
			@Override
			public void run() {
				long shutDownTime = System.currentTimeMillis();
				logger.info("Attempting to close all services and httpclient...");
				try {
					downloader.close();
					logger.info("Downloader closed");
					linkExtracator.close();
					logger.info("LinkExtractor closed");
					asyncHttpClient.close();
					logger.info("HttpClient closed");
					hashRing.close();
					logger.info("Zookeeper closed");
					executorService.shutdown();
					logger.info("ThreadPool closed");
					
					logger.info("------------------------------------------------------");
					logger.info("Total initiatied request: {}", stats.sendRequest.get());
					logger.info("Downloaded: {}", stats.downloadedPage.get());
					logger.info("Downloaded bytes: {}", stats.downloadedBytes.get());
					logger.info("Failed requests: {}", stats.failedRequest.get());
					logger.info("Timeout requests: {}", stats.timeoutFailure.get());
					logger.info("Connect refused: {}", stats.connectRefused.get());
					logger.info("Other exception: {}", stats.otherException.get());
					logger.info("Time span: {}s", (shutDownTime - startTime) / 1000.0);
					logger.info("------------------------------------------------------");
					
					executorService.awaitTermination(1, TimeUnit.MINUTES);
				} catch (IOException | InterruptedException e) {
					logger.error(e.getMessage());
				}
				
			}
		}));
	}
	
	public static String getNetWorkInterface() throws SocketException {
		String ipAddress = null;
		for (NetworkInterface i : Collections.list(NetworkInterface.getNetworkInterfaces())) {
			if (! i.isUp())
				continue;
			
			if (i.isLoopback())
				continue;
			
			if (i.isVirtual()) 
				continue;
			
			for (InetAddress address : Collections.list(i.getInetAddresses())) {
				ipAddress = address.getHostAddress();
				if (address instanceof Inet4Address) {
					return ipAddress;
				}
			}
		}
		
		return ipAddress;
		
	}
	
	
}
