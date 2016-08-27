package ai.vital.aspen.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertBlockToSequenceTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertSequenceToBlockTask;
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask;
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask;
import ai.vital.aspen.groovy.data.tasks.SaveDataSetTask;
import ai.vital.aspen.groovy.task.AbstractTask;
import ai.vital.vitalservice.ServiceOperations;

public class DowngradeUpgradeProcedureSteps {

	private String inputPath;
	private String outputPath;
	private ServiceOperations ops;
	private boolean overwrite;
	private Map<String, Object> paramsMap;
	
	protected String inputDatasetName = "upgrade-downgrade-input-dataset";
	protected String outputDatasetName = "upgrade-downgrade-output-dataset";
	private String builderContents;

	public DowngradeUpgradeProcedureSteps(Map<String, Object> globalParamsMap, String inputPath, String outputPath, ServiceOperations ops, String builderContents, boolean overwrite) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.ops = ops;
		this.builderContents = builderContents;
		this.overwrite = overwrite;
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
			String datasetSequencePath = AspenGroovyConfig.get().datasetsLocation + "/" + inputDatasetName + "/" + inputDatasetName + ".vital.seq";
			
			tasks.add(new DeletePathTask(datasetSequencePath, paramsMap));

			tasks.add(new ConvertBlockToSequenceTask(Arrays.asList(inputPath), datasetSequencePath, paramsMap));
			
			tasks.add(new LoadDataSetTask(paramsMap, datasetSequencePath, inputDatasetName));
			
		}
		
		
		//dataset ready, do convert
		
		
		if(outputPath.startsWith("name:")) {
			
			outputDatasetName = outputPath.substring(5);
			
			tasks.add(new DowngradeUpgradeProcedureTask(paramsMap, inputDatasetName, outputDatasetName, ops, builderContents));
			
		} else if(outputPath.endsWith(".vital") || outputPath.endsWith(".vital.gz")) {
			
			String datasetSequencePath = AspenGroovyConfig.get().datasetsLocation + "/" + outputDatasetName + "/" + outputDatasetName + ".vital.seq";
			
			//don't checkout output, always overwrite
			DeletePathTask deleteTask = new DeletePathTask(datasetSequencePath, paramsMap);
			tasks.add(deleteTask);
			
			
			tasks.add(new DowngradeUpgradeProcedureTask(paramsMap, inputDatasetName, outputDatasetName, ops, builderContents));
			
			SaveDataSetTask sdst = new SaveDataSetTask(paramsMap, outputDatasetName, datasetSequencePath);
			tasks.add(sdst);
			
			
			ConvertSequenceToBlockTask convertTask = new ConvertSequenceToBlockTask(Arrays.asList(datasetSequencePath), outputPath, paramsMap);
			
			tasks.add(convertTask);
			
		} else if(outputPath.endsWith(".vital.seq")){
			
			tasks.add(new DowngradeUpgradeProcedureTask(paramsMap, inputDatasetName, outputDatasetName, ops, builderContents));
			
			SaveDataSetTask sdst = new SaveDataSetTask(paramsMap, outputDatasetName, outputPath);
			tasks.add(sdst);
			
		} else {
			throw new RuntimeException("Expected input to be either a named RDD, vital seq or block file");
		}
		
		return tasks;
		
	}

	public String getBuilderContents() {
		return builderContents;
	}
	
	
	
}
