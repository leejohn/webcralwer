package jianli.ustc.paper;

import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
interface UrlSender extends Remote {

	void send(Collection<String> urls) throws RemoteException;
}

class UrlSenderImpl implements UrlSender {

	private BlockingQueue<String> linkQueue;

	public UrlSenderImpl(BlockingQueue<String> linkQueue) {
		this.linkQueue = linkQueue;
	}

	public void send(Collection<String> urls) throws RemoteException {
		this.process(urls);

	}

	private void process(Collection<String> urls) {
		linkQueue.addAll(urls);
	}

}

public class UrlSenders {
	private static Logger logger = LoggerFactory.getLogger(UrlSenders.class);
	private class SendTask implements Callable<Boolean> {
		private String host;
		private String name;
		private Collection<String> urls;

		/**
		 * 
		 * @param host
		 *            remote machine
		 * @param name
		 *            the remote object's name in rmiregistry
		 * @param urls
		 *            urls to be sent
		 */
		public SendTask(String host, String name, Collection<String> urls) {
			this.host = host;

			this.name = name;
			this.urls = urls;

		}

		public void run() {
			Iterator<String> iter = UrlSenders.this.hashRing
					.iterator(this.host);
			while (iter.hasNext()) {

				String node = iter.next();

				if ( node.equalsIgnoreCase(UrlSenders.this.self) || node.equalsIgnoreCase(UrlSenders.this.selfHost)) {
					logger.info("Put into myself's linkqueue");
					UrlSenders.this.linkQueue.addAll(urls);
					break;
				} else {
					try {
						UrlSender urlSender = UrlSenders.this.urlSenders
								.get(node);

						if (urlSender == null) {
							String[] destMachine = node.split(":");
							urlSender = createUrlSender(destMachine[0],
									destMachine[1]);

						}
						urlSender.send(urls);
					} catch (NumberFormatException | RemoteException
							| NotBoundException e) {
						logger.error("Get Remote machine {} failed {}", node, e);
						logger.info("Try next node");
						continue;
					}
				}
			}

		}

		private UrlSender createUrlSender(String remoteHost, String remotePort)
				throws AccessException, NumberFormatException, RemoteException,
				NotBoundException {
			UrlSender urlSender = (UrlSender) LocateRegistry.getRegistry(
					remoteHost, Integer.valueOf(remotePort)).lookup(name);
			urlSenders.put(remoteHost + ":" + remotePort, urlSender);
			return urlSender;
 
		}

		@Override
		public Boolean call() throws Exception {
			this.run();
			return true;
		}

	}

	private ExecutorService executorService;
	private ConcurrentHashMap<String, UrlSender> urlSenders;
	private HashRingFromZK hashRing;
	private BlockingQueue<String> linkQueue;
	private String selfHost;
	private String selfPort;
	private String name;
	private String self;

	public UrlSenders(ExecutorService executorService, HashRingFromZK hashRing,
			BlockingQueue<String> linkQueue, String specialSelf, String selfHost, String selfPort, String name) {
		this.executorService = executorService;
		this.urlSenders = new ConcurrentHashMap<String, UrlSender>();
		this.hashRing = hashRing;
		this.linkQueue = linkQueue;
		this.selfHost = selfHost;
		this.selfPort = selfPort;
		this.self = specialSelf;
		this.name = name;
	}

	public void send(final Collection<String> urls) {
		HashMap<String, List<String>> hostMap = new HashMap<String, List<String>>();

		for (String url : urls) {
			try {
				URL u = new URL(url);
				String host = u.getHost();
				List<String> urlList = hostMap.get(host);

				if (urlList == null) {
					urlList = new ArrayList<>();
					hostMap.put(host, urlList);
				}

				urlList.add(url);

			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}

		for (Entry<String, List<String>> entry : hostMap.entrySet()) {
			String key = entry.getKey();
			
			String node = hashRing.get(key);

			if (node.equalsIgnoreCase(this.self) || node.equalsIgnoreCase(selfHost)) {
				this.linkQueue.addAll(urls);
			} else {
				executorService.submit(new SendTask(key, name, entry.getValue()));

			}
			
		}
	}

}
