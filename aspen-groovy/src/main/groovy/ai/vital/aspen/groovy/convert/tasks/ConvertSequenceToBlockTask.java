package ai.vital.aspen.groovy.convert.tasks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class ConvertSequenceToBlockTask extends AbstractTask {

	public final static String VITAL_SEQUENCE_TO_BLOCK_PREFIX = "vital-sequence-to-block-"; 
	
	public List<String> inputPaths;
	
	public String outputPath;

	public ConvertSequenceToBlockTask(List<String> inputPaths, String outputPath, Map<String, Object> globalParameters) {
		super(globalParameters);
		
		this.inputPaths = inputPaths;
		this.outputPath = outputPath;
		
	}

	@Override
	public List<String> getRequiredParams() {

		List<String> l = new ArrayList<String>();
		
		for(String path : inputPaths) {
			
//			l.add(CheckPathTask.PATH_EXISTS_PREFIX + path);
			
		}
		return l;
		
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(VITAL_SEQUENCE_TO_BLOCK_PREFIX + outputPath);
	}

}
