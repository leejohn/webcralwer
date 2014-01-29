package test.asynchttp;

import static java.lang.System.out;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.rmi.registry.LocateRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;

public class TestIp {

	  public static void main(String args[]) throws SocketException {
	        Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
	        for (NetworkInterface netint : Collections.list(nets))
	            displayInterfaceInformation(netint);
	    }

	    static void displayInterfaceInformation(NetworkInterface netint) throws SocketException {
	        out.printf("Display name: %s\n", netint.getDisplayName());
	        out.printf("Name: %s\n", netint.getName());
	        Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
	        
	        for (InetAddress inetAddress : Collections.list(inetAddresses)) {
	        	out.println(inetAddress.getClass());
	        	out.println(inetAddress.getHostAddress());
	            out.printf("InetAddress: %s\n", inetAddress);
	        }
	       System.out.println();
	        out.printf("Up? %s\n", netint.isUp());
	        out.printf("Loopback? %s\n", netint.isLoopback());
	        out.printf("PointToPoint? %s\n", netint.isPointToPoint());
	        out.printf("Supports multicast? %s\n", netint.supportsMulticast());
	        out.printf("Virtual? %s\n", netint.isVirtual());
	        out.printf("Hardware address: %s\n",
	                    Arrays.toString(netint.getHardwareAddress()));
	        out.printf("MTU: %s\n", netint.getMTU());
	        out.printf("\n");
	        
	     }

}
