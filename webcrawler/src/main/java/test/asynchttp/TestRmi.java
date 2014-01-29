package test.asynchttp;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TestRmi {
	public static void main(String[] args) {
//		System.setProperty("java.rmi.server.useCodebaseOnly", "false");
//		System.setProperty("java.rmi.server.codebase", "file:////home/jianli/eclipse/workspace/crawler/target/classes/");
		System.setProperty("java.security.policy", "file:///home/jianli/eclipse/workspace/crawler/src/main/java/all.policy");
		System.out.println(System.getProperty("java.rmi.server.useCodebaseOnly"));
		System.out.println(System.getProperty("java.rmi.server.codebase"));
		System.out.println(System.getProperty("java.security.policy"));
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}
		
		EchoServer server = new EchoServerImpl();
		try {
			EchoServer serverStub = (EchoServer) UnicastRemoteObject.exportObject(server, 0);
			Registry registry = LocateRegistry.createRegistry(1099);
			System.out.println(registry.getClass());
			registry.rebind("echo", serverStub);
			
			System.out.println("Bound");
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

}
interface EchoServer extends Remote {
	List<String> say(List<String> greeting) throws RemoteException;
}

class EchoServerImpl implements EchoServer {

	public EchoServerImpl(){}
	
	public List<String> say(List<String> greeting) throws RemoteException {
		System.out.println(greeting);
		return greeting;
	}
	
}