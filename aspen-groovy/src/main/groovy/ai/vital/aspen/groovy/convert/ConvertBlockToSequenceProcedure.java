package ai.vital.aspen.groovy.convert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertBlockToSequenceTask;
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask;
import ai.vital.aspen.groovy.task.AbstractTask;

public class ConvertBlockToSequenceProcedure {

	public List<String> inputPaths;
	public String outputPath;
	public boolean overwrite;
	public Map<String, Object> paramsMap;

	public ConvertBlockToSequenceProcedure(List<String> inputPaths, String outputPath, boolean overwrite, Map<String, Object> globalParamsMap) {
		this.inputPaths = inputPaths;
		this.outputPath = outputPath;
		this.overwrite = overwrite;
		this.paramsMap = globalParamsMap;
	}
	
	public List<AbstractTask> generateTasks() {
		
		List<AbstractTask> l = new ArrayList<AbstractTask>();
		
		for(String inputPath : inputPaths) {
			
			CheckPathTask cpt = new CheckPathTask(inputPath, paramsMap);
			
			cpt.acceptDirectories = true;
			cpt.acceptFiles = true;
			cpt.mustExist = true;
			cpt.mustnotExist = false;
			cpt.validFileExtensions = new String[]{".vital", ".vital.gz"};
				
			
			l.add(cpt);
			
		}
		
		CheckPathTask cpt = new CheckPathTask(outputPath, paramsMap);
		cpt.mustExist = false;
		cpt.mustnotExist = !overwrite;
		cpt.acceptDirectories = true;
		cpt.validFileExtensions = new String[]{".vital.seq"};
		
		l.add(cpt);
		
		if(overwrite) {
			
			l.add(new DeletePathTask(outputPath, paramsMap));
			
		}
		
		l.add(new ConvertBlockToSequenceTask(inputPaths, outputPath, paramsMap));
		
		return l;
		
		
		
	}
	
}
