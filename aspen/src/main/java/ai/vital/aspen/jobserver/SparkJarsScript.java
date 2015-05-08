package ai.vital.aspen.jobserver;

import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ai.vital.aspen.jobserver.client.JobServerClient;

public class SparkJarsScript extends AbstractJobServerScript {

	static Map<String, Options> cmd2CLI = new LinkedHashMap<String, Options>();
	
	static String SJC = "spark-jars"; 
	
	static String CMD_LIST_JARS = "list";
	
	static String CMD_POST = "post";
	
	static Option appNameOption = new Option("app", "appName", true, "appName (required)");
	
	static Option jarPathOption = new Option("j", "jar", true, "jar path (required)");
	
	static {
		
		jobServerOption.setRequired(false);
		appNameOption.setRequired(true);
		jarPathOption.setRequired(true);
		
		Options listAppsOptions = new Options()
			.addOption(jobServerOption);
		cmd2CLI.put(CMD_LIST_JARS, listAppsOptions);
		
		Options postOptions = new Options()
			.addOption(jobServerOption)
			.addOption(appNameOption)
			.addOption(jarPathOption);
		cmd2CLI.put(CMD_POST, postOptions);
		
	}
	
	
	public static void main(String[] args) {

		String cmd = args.length > 0 ? args[0] : null;
		
		boolean printHelp = args.length == 0;
		
		if(printHelp) {
			usage(SJC, cmd2CLI);
			return;
		}
		
		String[] params = args.length > 1 ? Arrays.copyOfRange(args, 1, args.length) : new String[0];
				
		Options cli = cmd2CLI.get(cmd);
			
		if(cli == null) {
			e("unknown command: " + cmd);
			usage(SJC, cmd2CLI);
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

		
		if(CMD_LIST_JARS.equals(cmd)) {
			
			o("Listing jars...");

			LinkedHashMap<String, Object> jars_get = null;
			
			try {
				jars_get = client.jars_get();
			} catch (Exception e) {
				e(e.getLocalizedMessage());
				return;
			}
			
			o("Jars count: " + jars_get.size());
			
			int i = 1;
			
			for(Entry<String, Object> jar : jars_get.entrySet() ) {
				
				o(i + ".   " + jar.getKey() + "   " + jar.getValue());
				
			}
			
		} else if(CMD_POST.equals(cmd)) {
			
			String appName = options.getOptionValue(appNameOption.getLongOpt());
			
			Path jarPath = new Path(options.getOptionValue(jarPathOption.getLongOpt()));
			
			o("Uploading app jar: " + appName + " path: " + jarPath + " ... ");
			
			InputStream jarInputStream = null;
			
			try {
				
				FileSystem jarFS = FileSystem.get(jarPath.toUri(), new Configuration()); 
				
				FileStatus fs = jarFS.getFileStatus(jarPath);
				
				if(!fs.isFile()) throw new Exception("Jar path exists but is not a file: " + jarPath);
				
				jarInputStream = jarFS.open(jarPath);
				
				String jars_post = client.jars_post(appName, jarInputStream);
				
				o(jars_post);
				
			} catch (Exception e) {
				e(e.getLocalizedMessage());
				return;
			} finally {
				IOUtils.closeQuietly(jarInputStream);
			}
			
		} else {
			e("Unhandled command: " + cmd);
		}
		
	}

}
