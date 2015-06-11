package ai.vital.aspen.jobserver;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import ai.vital.aspen.jobserver.client.JobServerClient;

public class SparkContextsScript extends AbstractJobServerScript {

static Map<String, Options> cmd2CLI = new LinkedHashMap<String, Options>();
	
	static String SCC = "spark-contexts"; 
	
	static String CMD_LIST_CONTEXTS = "list";
	
	static String CMD_DELETE = "delete";
	
	static String CMD_POST = "post";
	
	static Option contextOption = new Option("c", "context", true, "spark context name");
	
	static Option paramsOption = new Option("p", "params", true, "optional spark context params string (url-encoded)");
	
	static {
		
		jobServerOption.setRequired(false);
		contextOption.setRequired(true);
		paramsOption.setRequired(false);
		
		Options listContextsOptions = new Options()
			.addOption(jobServerOption);
		cmd2CLI.put(CMD_LIST_CONTEXTS, listContextsOptions);
		
		Options deleteOptions = new Options()
			.addOption(jobServerOption)
			.addOption(contextOption);
		cmd2CLI.put(CMD_DELETE, deleteOptions);
		
		Options postOptions = new Options()
			.addOption(jobServerOption)
			.addOption(contextOption)
			.addOption(paramsOption);
		cmd2CLI.put(CMD_POST, postOptions);
		
	}
	
	
	public static void main(String[] args) {

		String cmd = args.length > 0 ? args[0] : null;
		
		boolean printHelp = args.length == 0;
		
		if(printHelp) {
			usage(SCC, cmd2CLI);
			return;
		}
		
		String[] params = args.length > 1 ? Arrays.copyOfRange(args, 1, args.length) : new String[0];
				
		Options cli = cmd2CLI.get(cmd);
			
		if(cli == null) {
			e("unknown command: " + cmd);
			usage(SCC, cmd2CLI);
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

		
		if(CMD_LIST_CONTEXTS.equals(cmd)) {
			
			o("Listing contexts...");
			
			List<String> ctxs = null; 
			try {
				ctxs = client.contexts_get();
			} catch (Exception e) {
				e(e.getLocalizedMessage());
				return;
			}
			
			o("Contexts count: " + ctxs.size());
			
			for(int i = 1; i <= ctxs.size(); i++) {
				
				o(i + ".   " + ctxs.get(i-1));
				
			}
			
		} else if(CMD_POST.equals(cmd)) {
			
			String context = options.getOptionValue(contextOption.getLongOpt());
			
			String contextParamsURLEncoded = options.getOptionValue(paramsOption.getLongOpt());

			if(contextParamsURLEncoded != null) {
				o("optional context params URL encoded string:\n" + contextParamsURLEncoded);
			}
			o("Posting new context: " + context + " ... ");
			
			try {
				String contexts_post = client.contexts_post(context, contextParamsURLEncoded);
				o(contexts_post);
			} catch (Exception e) {
				e(e.getLocalizedMessage());
			}
			
		} else if(CMD_DELETE.equals(cmd)) {
			
			String context = options.getOptionValue(contextOption.getLongOpt());
			
			o("Deleting context: " + context + " ...");
			
			try {
				o( client.contexts_delete(context) );
			} catch (Exception e) {
				e(e.getLocalizedMessage());
			}
			
		} else {
			e("Unhandled command: " + cmd);
		}
		
	}

}
