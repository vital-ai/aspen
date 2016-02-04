package ai.vital.aspen.jobserver;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ai.vital.aspen.jobserver.client.JobServerClient;

import com.typesafe.config.ConfigFactory;

public class SparkJobsScript extends AbstractJobServerScript {

	static Map<String, Options> cmd2CLI = new LinkedHashMap<String, Options>();
	
	static String SJC = "spark-jobs"; 
	
	static String CMD_LIST_JOBS = "list";
	
	static String CMD_GET_DETAILS = "get";
	
	static String CMD_DELETE = "delete";
	
	static String CMD_POST = "post";
	
	static Option jobIdOption = new Option("jid", "jobId", true, "jobId (required)");
	
	static Option appNameOption = new Option("app", "appName", true, "appName - uploaded jar name (required)");
	
	static Option classPathOption = new Option("cp", "classPath", true, "classPath - job class canonical name (required)");
	
	static Option contextOption = new Option("c", "context", true, "optional spark context name");
	
	static Option syncOption = new Option("s", "sync", false, "run job synchronously (wait for results) (optional)");
	
	static Option paramsFileOption = new Option("pf", "params-file", true, "input job params hocon/json file path (mutually exclusive with --params)");
	
	static Option paramsOption = new Option("p", "params", true, "input job params string (mutually exclusive with --params-file)");
	
	static {
		
		jobServerOption.setRequired(false);
		jobIdOption.setRequired(true);
		appNameOption.setRequired(true);
		classPathOption.setRequired(true);
		contextOption.setRequired(false);
		syncOption.setRequired(false);
		paramsFileOption.setRequired(false);
		paramsOption.setRequired(false);
		
		Options listJobsOptions = new Options()
			.addOption(jobServerOption);
		cmd2CLI.put(CMD_LIST_JOBS, listJobsOptions);
		
		Options jobDetailsOptions = new Options()
			.addOption(jobServerOption)
			.addOption(jobIdOption);
		cmd2CLI.put(CMD_GET_DETAILS, jobDetailsOptions);
		
		Options deleteOptions = new Options()
			.addOption(jobServerOption)
			.addOption(jobIdOption)
			;
		cmd2CLI.put(CMD_DELETE, deleteOptions);
		
		Options postOptions = new Options()
			.addOption(jobServerOption)
			.addOption(appNameOption)
			.addOption(classPathOption)
			.addOption(contextOption)
			.addOption(syncOption)
			.addOption(paramsOption)
			.addOption(paramsFileOption);
		
		cmd2CLI.put(CMD_POST, postOptions);
		
	}
	
	public static void main(String[] args) throws ParseException {

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
		
		
		if(CMD_LIST_JOBS.equals(cmd)) {
			
			o("Listing jobs...");
			
			List<LinkedHashMap<String, Object>> r = null;
			
			try {
				r = client.jobs_get();
			} catch (Exception e) {
				e( e.getMessage() );
				return;
			}
			
			o("All jobs count: " + r.size());
			
			int c = 1;
			for(LinkedHashMap<String, Object> job : r ) {
				
				o("#" + c);
				o("jobId: " + job.get("jobId"));
				o("status: " + job.get("status"));
				o("classPath: " + job.get("classPath"));
				o("context: " + job.get("context"));
				o("startTime: " + job.get("startTime"));
				o("duration: " + job.get("duration"));
				
				c++;
				
			}
			
		} else if(CMD_GET_DETAILS.equals(cmd)) {
			
			String jobId = options.getOptionValue(jobIdOption.getLongOpt());
			
			o("Getting job " + jobId + " details...");
			
			try {
				
				LinkedHashMap<String, Object> details = client.jobs_get_details(jobId);
				o ( mapper.defaultPrettyPrintingWriter().writeValueAsString(details) );
				
			} catch (Exception e) {
				e(e.getLocalizedMessage());
				return;
			}
			
		} else if(CMD_DELETE.equals(cmd)) {
			
			String jobId = options.getOptionValue(jobIdOption.getLongOpt());
			
			o("Deleting job " + jobId + " ...");
			
			try {
				
				LinkedHashMap<String, Object> status = client.jobs_delete(jobId);
				
				o ( mapper.defaultPrettyPrintingWriter().writeValueAsString(status) );
				
			} catch(Exception e) {
				e(e.getLocalizedMessage());
			}
			
		} else if(CMD_POST.equals(cmd)) {
			
			o("Posting new job ...");
			
			String appName = options.getOptionValue(appNameOption.getLongOpt());
			
			String classPath = options.getOptionValue(classPathOption.getLongOpt());
			
			String context = options.getOptionValue(contextOption.getLongOpt());
			
			Boolean sync = options.hasOption(syncOption.getLongOpt());
			
			o("appName: " + appName);
			o("classPath: " + classPath);
			o("context: " + context);
			o("sync ? " + sync);
			
			String paramsString = options.getOptionValue(paramsOption.getLongOpt());
			String paramsFile = options.getOptionValue(paramsFileOption.getLongOpt());
			
			if(paramsString != null && paramsFile != null) {
				e("cannot provide both params string and file, exactly 1 form expected");
				return;
			} 
			
			if(paramsString == null && paramsFile == null) {
				e("no params params string nor file provided, exactly 1 form expected");
				return;
			} 

			if(paramsFile != null) {
				Path paramsFilePath = new Path(paramsFile);
				o("loading params string from path: " + paramsFilePath.toString());
				try {
					FileSystem fileSystem = FileSystem.get(paramsFilePath.toUri(), new Configuration());
					FileStatus fileStatus = fileSystem.getFileStatus(paramsFilePath);
					if(!fileStatus.isFile()) throw new Exception("Path is not a file: " + fileStatus);
					
					FSDataInputStream stream = fileSystem.open(paramsFilePath);
					paramsString = IOUtils.toString(stream, StandardCharsets.UTF_8.name());
					stream.close();
					fileSystem.close();
				} catch(Exception e) {
					e(e.getLocalizedMessage());
					return;
				}
				
				o("loaded the following params:\n" + paramsString);
				
			} else {
				o("using provded params string:\n" + paramsString);
			}
			
			try {
				ConfigFactory.parseString(paramsString);
			} catch(Exception e) {
				e("Job parameters are not valid: " + e.getLocalizedMessage());
				return;
			} 
			
			
			try {
				
				LinkedHashMap<String, Object> status = client.jobs_post(appName, classPath, context, sync, paramsString);
				
				o ( mapper.defaultPrettyPrintingWriter().writeValueAsString(status) );
				
			} catch(Exception e) {
				e(e.getLocalizedMessage());
			}
			
		} else {
			e("Unhandled command: " + cmd);
		}
	
	}

}
