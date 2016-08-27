package ai.vital.aspen.jobserver;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.codehaus.jackson.map.ObjectMapper;

import ai.vital.aspen.config.AspenConfig;
import ai.vital.aspen.jobserver.client.JobServerClient;

public abstract class AbstractJobServerScript {

	static Option jobServerOption = new Option("js", "jobserver", true, "optional job-server location (URL), http:// protocol prefix is optional, if not specified the $VITAL_HOME/vital-config/aspen/aspen.config is used.");
	
	static ObjectMapper mapper = new ObjectMapper();
	
	static BasicParser parser = new BasicParser();
	
	static void usage(String cmd, Map<String, Options> cmd2CLI) {
		
		o( "usage: " + cmd + " <command> [options] ..." );
		
		HelpFormatter hf = new HelpFormatter();
		for(Entry<String, Options> e : cmd2CLI.entrySet()) {
			hf.printHelp(cmd + " " + e.getKey(), e.getValue());
		}
		
	}
	
	static void o(Object o) { System.out.println(o); }
	
	static void e(Object o) { System.err.println(o); }

	static JobServerClient initClient(CommandLine options) throws Exception {
		
		String jobServerLocation = options.getOptionValue(jobServerOption.getLongOpt());
		
		if(jobServerLocation != null) {
			o("Custom job server location: " + jobServerLocation);
			if(jobServerLocation.isEmpty()) {
				throw new Exception(jobServerOption.getOpt() + " param cannot be empty");
				
			}
		} else {
			o("Getting default job server location ...");
			AspenConfig cfg = AspenConfig.get();
			jobServerLocation = cfg.getJobServerURL();
			if(jobServerLocation == null || jobServerLocation.isEmpty()) {
				throw new Exception("no default jobserver location found");
			}
			o("default job server location: " + jobServerLocation);
		}
		
		return new JobServerClient(jobServerLocation);
	}
}
