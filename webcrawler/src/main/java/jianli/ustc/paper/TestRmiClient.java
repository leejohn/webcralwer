package jianli.ustc.paper;

import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

public class TestRmiClient {

	public static void main(String[] args) throws MalformedURLException, RemoteException, NotBoundException {
		
		System.out.println(System.getProperty("java.rmi.server.hostname"));
		Registry registry = LocateRegistry.getRegistry("192.168.1.106");
		System.out.println(registry.getClass());
		UrlSender sender = (UrlSender) registry.lookup("crawler");
		System.out.println(sender.getClass());
		List<String> urls = new ArrayList<String>();
		urls.add("http://www.sina.com.cn");
		sender.send(urls);
		for (String s : registry.list())
			System.out.println(s);
	}

}
