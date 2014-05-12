package test.asynchttp;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

public class Args4jTest {

	@Option(name="-t", usage="Specify the time for cralwer to run")
	public int time;
	@Option(name="-seeds", handler=StringArrayOptionHandler.class)
	public String[] seeds;
	
	public static void main(String[] args) throws CmdLineException {
		new Args4jTest(args);
	}
	
	public Args4jTest(String[] args) throws CmdLineException {
		CmdLineParser parser = new CmdLineParser(this);
		parser.parseArgument(args);
		
		Executors.newSingleThreadScheduledExecutor().schedule(new Runnable() {
			
			@Override
			public void run() {
				System.out.println("Closing program");
				System.exit(0);
				
			}
		}, time, TimeUnit.SECONDS);
		
		for (String s : seeds) {
			System.out.println(s);
		}
	}

}
