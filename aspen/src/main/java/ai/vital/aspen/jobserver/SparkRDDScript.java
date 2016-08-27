package ai.vital.aspen.jobserver;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import ai.vital.aspen.jobserver.client.JobServerClient;

public class SparkRDDScript extends AbstractJobServerScript {

	static Map<String, Options> cmd2CLI = new LinkedHashMap<String, Options>();
	
	static String SRC = "spark-rdds"; 
	
	static String CMD_LIST_RDDS = "list";
	
	static String CMD_REMOVE = "remove";
	
	static Option appNameOption = new Option("app", "appName", true, "appName (required)");
	
	static Option contextOption = new Option("c", "context", true, "spark context name (required)");
	
	static Option nameOption = new Option("n", "rdd-name", true, "RDD name (required)");
	
	static {
		
		jobServerOption.setRequired(false);
		appNameOption.setRequired(true);
		contextOption.setRequired(true);
		nameOption.setRequired(true);
		
		Options listRDDsOptions = new Options()
			.addOption(jobServerOption)
			.addOption(appNameOption)
			.addOption(contextOption);
		
		cmd2CLI.put(CMD_LIST_RDDS, listRDDsOptions);
		
		Options postOptions = new Options()
			.addOption(jobServerOption)
			.addOption(appNameOption)
			.addOption(contextOption)
			.addOption(nameOption);
		cmd2CLI.put(CMD_REMOVE, postOptions);
		
	}
	
	
	public static void main(String[] args) {

		String cmd = args.length > 0 ? args[0] : null;
		
		boolean printHelp = args.length == 0;
		
		if(printHelp) {
			usage(SRC, cmd2CLI);
			return;
		}
		
		String[] params = args.length > 1 ? Arrays.copyOfRange(args, 1, args.length) : new String[0];
				
		Options cli = cmd2CLI.get(cmd);
			
		if(cli == null) {
			e("unknown command: " + cmd);
			usage(SRC, cmd2CLI);
			return;
		}
		
		CommandLine options = null;
		try {
			options = parser.parse(cli, params);
		} catch(Exception e) {
			e(e.getLocalizedMessage());
			return;
		}
		
		
		JobServerClient client = null;
		try {
			client = initClient(options);
		} catch (Exception e1) {
			e(e1.getLocalizedMessage());
			return;
		} 

		
		String appName = options.getOptionValue(appNameOption.getLongOpt());
		String context = options.getOptionValue(contextOption.getLongOpt());
				
		o("app: " + appName);
		o("context: " + context);
		
		if(CMD_LIST_RDDS.equals(cmd)) {
			
			o("Listing named RDDs ...");

			String paramsString = "" +
					"action: list\n";
			
			try {
				LinkedHashMap<String, Object> results = client.jobs_post(appName, SparkRDDJob.class.getCanonicalName(), context, true, paramsString);
				
				o ( mapper.defaultPrettyPrintingWriter().writeValueAsString(results) );
				
			} catch (Exception e) {
				e(e.getLocalizedMessage());
				return;
			}
			
		} else if(CMD_REMOVE.equals(cmd)) {

			String rddName = options.getOptionValue(nameOption.getLongOpt());
			
			o("Removing RDD with name: "+ rddName + " ...");
			
			String paramsString = "" +
					"action: remove\n" +
					"rdd-name: \"" + rddName + "\""
					;
			
			try {
				LinkedHashMap<String, Object> results = client.jobs_post(appName, SparkRDDJob.class.getCanonicalName(), context, true, paramsString);
				
				o ( mapper.defaultPrettyPrintingWriter().writeValueAsString(results) );
				
			} catch (Exception e) {
				e(e.getLocalizedMessage());
				return;
			}
			
		} else {
			e("Unhandled command: " + cmd);
		}
		
	}
	
}
