package jianli.ustc.paper;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Statistics {
	public AtomicInteger downloadedPage;
	public AtomicInteger failedRequest;
	public AtomicLong downloadedBytes;
	public AtomicInteger timeoutFailure;
	public AtomicInteger connectRefused;
	public AtomicInteger otherException;
	public AtomicInteger sendRequest;
	
	{
		downloadedPage = new AtomicInteger();
		failedRequest = new AtomicInteger();
		downloadedBytes = new AtomicLong();
		connectRefused = new AtomicInteger();
		otherException = new AtomicInteger();
		timeoutFailure = new AtomicInteger();
		sendRequest = new AtomicInteger();
	}
}
