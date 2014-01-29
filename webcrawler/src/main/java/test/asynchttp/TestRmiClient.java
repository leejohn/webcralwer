package test.asynchttp;

import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

public class TestRmiClient {

	public static void main(String[] args) throws MalformedURLException, RemoteException, NotBoundException {
		Registry registry = LocateRegistry.getRegistry("localhost");
		System.out.println(registry.getClass());
		EchoServer server = (EchoServer) registry.lookup("echo");
		List<String> greetings = new ArrayList<String>();
		greetings.add("sdfads");
		greetings.add("hello");
		greetings.add("yes");
		for (String s : registry.list())
			System.out.println(s);
	}

}
