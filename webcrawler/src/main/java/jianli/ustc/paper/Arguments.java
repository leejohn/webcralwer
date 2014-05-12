package jianli.ustc.paper;

import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

public class Arguments {
	@Option(name="-t", usage="Specify the time span for crawler to run")
	public int timeInSecond;
	
	@Option(name="-conns", usage="Specify the total connections for crawler")
	public int connections;
	
	@Option(name="-seeds", handler=StringArrayOptionHandler.class)
	public String[] seeds;
	
	@Option(name="-timeout", usage="Specify the timeout for request in second")
	public int timeoutInSeconds;
	
	public Arguments(int timeInSecond, int connections, int timeoutInSeconds) {
		this.timeInSecond = timeInSecond;
		this.connections = connections;
		this.timeoutInSeconds = timeoutInSeconds;
	}
	
	
	
}
