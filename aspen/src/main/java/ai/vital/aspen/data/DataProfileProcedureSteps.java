package ai.vital.aspen.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertBlockToSequenceTask;
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask;
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask;
import ai.vital.aspen.groovy.task.AbstractTask;

public class DataProfileProcedureSteps {

	private String inputPath;
	private String outputPath;
	private boolean overwrite;
	private Set<String> propertiesFilter;
	
	protected String inputDatasetName = "data-profile-input-dataset";
	private Map<String, Object> paramsMap;

	public DataProfileProcedureSteps(Map<String, Object> globalParamsMap, String inputPath, String outputPath, Set<String> propertiesFilter, boolean overwrite) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.overwrite = overwrite;
		this.propertiesFilter = propertiesFilter; 
		this.paramsMap = globalParamsMap;
	}

	public List<AbstractTask> generateTasks() throws Exception {
		
		List<AbstractTask> tasks = new ArrayList<AbstractTask>();
		
		if(overwrite) {
			
			tasks.add(new DeletePathTask(outputPath, paramsMap));
			
		} else {
			
			CheckPathTask cpt = new CheckPathTask(outputPath, paramsMap);
			cpt.mustnotExist = true;
			cpt.mustExist = false;
			cpt.acceptFiles = true;
			cpt.acceptFiles = true;
			
			tasks.add(cpt);
			
		}
		
		
		
		if(inputPath.startsWith("name:")) {

			inputDatasetName = inputPath.substring(5);
			
			paramsMap.put(inputDatasetName, true);
			
			tasks.add(new LoadDataSetTask(paramsMap, inputPath, inputDatasetName));
			
		} else if(inputPath.endsWith(".vital.seq")) {
			
			tasks.add(new LoadDataSetTask(paramsMap, inputPath, inputDatasetName));
			
		} else if(inputPath.endsWith(".vital") || inputPath.endsWith(".vital.gz")) {
			
			//convert it into sequence file first
			String datasetSequencePath = AspenGroovyConfig.get().datesetsLocation + "/" + inputDatasetName + "/" + inputDatasetName + ".vital.seq";
			
			tasks.add(new DeletePathTask(datasetSequencePath, paramsMap));

			tasks.add(new ConvertBlockToSequenceTask(Arrays.asList(inputPath), datasetSequencePath, paramsMap));
			
			tasks.add(new LoadDataSetTask(paramsMap, datasetSequencePath, inputDatasetName));
			
		}
		
		
		//dataset ready, do convert
		tasks.add(new DataProfileTask(paramsMap, inputDatasetName, outputPath, propertiesFilter));
		
		return tasks;
		
	}

}
