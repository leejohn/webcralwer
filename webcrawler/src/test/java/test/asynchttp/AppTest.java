package test.asynchttp;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     * @throws IOException 
     * @throws InterruptedException 
     * @throws KeeperException 
     */
    public void testApp() throws IOException, KeeperException, InterruptedException
    {
    	ZooKeeper zk = new ZooKeeper("localhost:2181", 3000, null);
		System.out.println(zk.exists("/", false));
		TimeUnit.SECONDS.sleep(1);
		int[] a = new int[]{123,5,45,46,7,56,78};
		System.out.println(zk.getChildren("/", null));
		zk.create("/zookeeper", new byte[]{Byte.decode("123")}, null, CreateMode.PERSISTENT);
		zk.delete("/spider-cluster/machine8:234", -1);
    }
}
