package jianli.ustc.paper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.activemq.apollo.util.HashRing;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashRingFromZK implements Watcher {
	private static Logger logger =  LoggerFactory.getLogger(HashRingFromZK.class);
	
	private HashRing<String, String> hashRing;
	private ZooKeeper zk;
	private String clusterPath;
	private String connectString;
	private int sessionTimeout;
	private Watcher watcher;
	private boolean canBeReadOnly;
	private long sessionId = -1;
	private byte[] sessionPasswd = null;
	private String self;
	
	public List<String> getNodes() {
		return hashRing.getNodes();
	}


	public ZooKeeper getZk() {
		return zk;
	}


	public String getClusterPath() {
		return clusterPath;
	}


	public void setClusterPath(String clusterPath) {
		this.clusterPath = clusterPath;
	}


	public String get(String resource) {
		return hashRing.get(resource);
	}


	public boolean remove(String node) {
		return hashRing.remove(node);
	}


	public Iterator<String> iterator(String resource) {
		return hashRing.iterator(resource);
	}


	public String toString() {
		return hashRing.toString();
	}
//
//	public HashRingFromZK(ZooKeeper zk, String clusterPath) {
//		this.zk = zk;
//		this.clusterPath = clusterPath;
//	}
	
	public HashRingFromZK(ZooKeeper zk, String clusterPath) {
		this.zk = zk;
		this.clusterPath = clusterPath;
	}
	
	public HashRingFromZK(String self, String connectString, int sessionTimeout, Watcher watcher,
            long sessionId, byte[] sessionPasswd, boolean canBeReadOnly, String clusterPath ) throws IOException {
		this.connectString = connectString;
		this.sessionTimeout = sessionTimeout;
		this.watcher = watcher;
		this.sessionId = sessionId;
		this.sessionPasswd = sessionPasswd;
		this.canBeReadOnly = canBeReadOnly;
		this.clusterPath = clusterPath;
		this.hashRing = new HashRing<String, String>();

	}
	
	public HashRingFromZK(String self, String connectString, int sessionTimeout, Watcher watcher,
             boolean canBeReadOnly, String clusterPath) {
		this.connectString = connectString;
		this.sessionTimeout = sessionTimeout;
		this.watcher = watcher;
		this.canBeReadOnly = canBeReadOnly;
		this.clusterPath = clusterPath;
		this.hashRing = new HashRing<String, String>();
		this.self = self;

	}
	
	private void createSpiderClusterPathIfNotExists() throws KeeperException, InterruptedException {
		if (zk.exists(this.clusterPath, null) == null) {
			zk.create(this.clusterPath, this.clusterPath.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}
	
	public void Initialize() throws InterruptedException {
		logger.info("HashRingFromZK initializing........");
		if (this.watcher == null) {
			this.watcher = new Watcher() {
				
				public void process(WatchedEvent event) {
					if (event.getState() == KeeperState.Expired) {
						logger.error("Connection expired, reconnect immediately");
						HashRingFromZK.this.hashRing.clear();
						HashRingFromZK.this.hashRing.add("self");
						
						try {
							HashRingFromZK.this.Initialize();
						} catch (InterruptedException e) {
							logger.error(e.toString());
						}
					} else if (event.getState() == KeeperState.SyncConnected) {
						try {
							logger.info("Zookeeper connected, try to create " + clusterPath + " if not exists");
							createSpiderClusterPathIfNotExists();
							logger.info("Zookeeper repopulate with nodes under {}", clusterPath);
							repopulateHashRing(true);
						} catch (KeeperException e) {
							logger.error(e.toString());
						} catch (InterruptedException e) {
							logger.error(e.toString());
						}
					} else if (event.getState() == KeeperState.Disconnected) {
						logger.info("Connection to ZooKeeper failed, repopulate with self");
						hashRing.clear();
						hashRing.add(self);
					}
				}
			};
		}
		
		try {
			
			if (sessionId == -1 && sessionPasswd == null) 
				this.zk = new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
			else
				this.zk = new ZooKeeper(connectString, sessionTimeout, watcher, sessionId, sessionPasswd, canBeReadOnly);
			
			// zookeeper might't throw some exception so we will first add self into hashring
			// in case that connecting to zookeeper failed, we have nothing in the hashring
			this.hashRing.add(this.self);
		} catch (IOException e) {
			this.handleInitializationException(e);
		}
		
		
	}
	
	private void handleInitializationException(Exception e) throws InterruptedException {
		logger.error(e.toString());
		this.hashRing.add(this.self);
//		Executors.newSingleThreadScheduledExecutor().schedule(new Runnable() {
//			
//			public void run() {
//				HashRingFromZK.this.Initialize();
//				
//			}
//		}, 1, TimeUnit.MINUTES);
	}
	
	protected List<String> registerWatchOnCluster() throws KeeperException, InterruptedException {
		List<String> children = this.zk.getChildren(this.clusterPath, this);
		return children;
	}
	
	protected List<String> repopulateHashRing(boolean watch) throws KeeperException, InterruptedException {
		this.hashRing.clear();
		List<String> children = this.zk.getChildren(this.clusterPath, watch ? this : null);
		
		this.hashRing.addAll(children);
		this.hashRing.add("self");
		
		logger.info("HashRing repopulated: {}", this.hashRing.getNodes());
		return children;
	}


	public void process(WatchedEvent event) {
		if (event.getType() ==  EventType.NodeChildrenChanged) {
			try {
				logger.info("Children changed");
				this.repopulateHashRing(true);
			} catch (Exception e) {
				logger.error(e.toString());
			}
		}
	}
	
	public void close() throws InterruptedException {
		if (this.zk != null)
			this.zk.close();
	}
	
}
