package ai.vital.aspen.groovy.convert.tasks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

/**
 * This task simply verifies if a path exists 
 * @author Derek
 *
 */
public class CheckPathTask extends AbstractTask {

	public final static String PATH_EXISTS_PREFIX = "path-exists-";
	
//	public List<String> paths;
	public String path;
	
	//config flags
	public boolean acceptFiles = true;
	
	public boolean acceptDirectories = true;

	public boolean mustExist = true;
	
	public boolean mustnotExist = false;
	
	//if empty accept everything
	public String[] validFileExtensions = null;
	
	//check if it's a single directory
	public boolean singleDirectory = false;
	
	public CheckPathTask(String path, Map<String, Object> globalParameters) {
		super(globalParameters);
//		this.paths = paths;
		this.path = path;
	}

	@Override
	public List<String> getRequiredParams() {
		return Collections.emptyList();
	}

	@Override
	public List<String> getOutputParams() {
		/*
		List<String> l = new ArrayList<String>();
		for( String path : paths ) {
			l.add( PATH_EXISTS_PREFIX + path) ;
		}
		
		return l;
		*/
		return Arrays.asList( PATH_EXISTS_PREFIX + path);
	}

}
