package ai.vital.aspen.groovy.convert.tasks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class DeletePathTask extends AbstractTask {

	public final static String PATH_DELETED_PREFIX = "path-deleted-";
	
	public String path;
	
	public DeletePathTask(String path, Map<String, Object> globalParameters) {
		super(globalParameters);
		this.path = path;
	}

	
	@Override
	public List<String> getRequiredParams() {
		return Collections.emptyList();
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(PATH_DELETED_PREFIX + path);
	}

}
