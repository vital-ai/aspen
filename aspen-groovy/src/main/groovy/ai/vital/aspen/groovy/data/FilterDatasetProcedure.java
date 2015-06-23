package ai.vital.aspen.groovy.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertBlockToSequenceTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertSequenceToBlockTask;
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask;
import ai.vital.aspen.groovy.data.tasks.FilterDatasetTask;
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask;
import ai.vital.aspen.groovy.data.tasks.SaveDataSetTask;
import ai.vital.aspen.groovy.task.AbstractTask;

public class FilterDatasetProcedure {

	public String inputPath;
	
	public String queryBuilderPath;
	
	public String outputPath;
	
	public boolean overwrite = false;
	
	public Map<String, Object> paramsMap;
	
	
	public String inputDatasetName = "filter-input-dataset";
	
	public String outputDatasetName = "filter-output-dataset";
	
	
	public FilterDatasetProcedure(String inputPath, String queryBuilderPath,
			String outputPath, boolean overwrite, Map<String, Object> paramsMap) {
		super();
		this.inputPath = inputPath;
		this.queryBuilderPath = queryBuilderPath;
		this.outputPath = outputPath;
		this.overwrite = overwrite;
		this.paramsMap = paramsMap;
	}



	public List<AbstractTask> generateTasks() throws Exception {
		
		List<AbstractTask> l = new ArrayList<AbstractTask>();
	
		CheckPathTask inputCheckTask = new CheckPathTask(inputPath, paramsMap);
		inputCheckTask.mustExist = true;
		inputCheckTask.mustnotExist = false;
		l.add(inputCheckTask);
		
		
		
		if(overwrite) {
			
			l.add(new DeletePathTask(outputPath, paramsMap));
			
		} else {
			
			CheckPathTask outCheckPathTask = new CheckPathTask(outputPath, paramsMap);
			outCheckPathTask.mustExist = false;
			outCheckPathTask.mustnotExist = !overwrite;
			l.add(outCheckPathTask);
			
		}
		
		
		if(inputPath.startsWith("name:")) {
		
			inputCheckTask.acceptFiles = true;
			inputCheckTask.acceptDirectories = true;
			
			inputDatasetName = inputPath.substring(5);
			
			LoadDataSetTask loadDataSetTask = new LoadDataSetTask(paramsMap, inputPath, inputDatasetName);
			l.add(loadDataSetTask);
			
		} else if(inputPath.endsWith(".vital.seq")) {
			
			inputCheckTask.acceptDirectories = true;
			inputCheckTask.acceptFiles = true;
			
			
			LoadDataSetTask loadDataSetTask = new LoadDataSetTask(paramsMap, inputPath, inputDatasetName);
			l.add(loadDataSetTask);
			
		} else if(inputPath.endsWith(".vital.gz") || inputPath.endsWith(".vital")) {
			
			inputCheckTask.acceptDirectories = false;
			inputCheckTask.acceptFiles = true;

			
			String datasetSequencePath = AspenGroovyConfig.get().datesetsLocation + "/" + inputDatasetName + "/" + inputDatasetName + ".vital.seq";
			
			//don't checkout output, always overwrite
			DeletePathTask deleteTask = new DeletePathTask(datasetSequencePath, paramsMap);
			l.add(deleteTask);
			
			
			ConvertBlockToSequenceTask convertTask = new ConvertBlockToSequenceTask(Arrays.asList(inputPath), datasetSequencePath, paramsMap);
			l.add(convertTask);
			
			
			LoadDataSetTask loadDataSetTask = new LoadDataSetTask(paramsMap, datasetSequencePath, inputDatasetName);
			l.add(loadDataSetTask);
			
		} else {
			throw new RuntimeException("Invalid input path: " + inputPath);
		}
		
		
		if(outputPath.startsWith("name:")) {
			outputDatasetName = outputPath.substring(5);
		}
		
		l.add(new FilterDatasetTask(paramsMap, inputDatasetName, outputDatasetName, queryBuilderPath));
		

		if(outputPath.startsWith("name:")) {
			
			//nothing to do, results will be persisted as namedRDD
			
		} else if(outputPath.endsWith(".vital.seq")) {
			
			l.add(new SaveDataSetTask(paramsMap, outputDatasetName, outputPath));
			
		} else if(outputPath.endsWith(".vital.gz") || outputPath.endsWith(".vital")) {

			//save and convert
			//dump to sequence file first
			String datasetSequencePath = AspenGroovyConfig.get().datesetsLocation + "/" + outputDatasetName + "/" + outputDatasetName + ".vital.seq";
			
			//delete saved dataset and dump and convert
			l.add(new DeletePathTask(datasetSequencePath, paramsMap));
			l.add(new SaveDataSetTask(paramsMap, outputDatasetName, datasetSequencePath));
			l.add(new ConvertSequenceToBlockTask(Arrays.asList(datasetSequencePath), outputPath, paramsMap )); 
			
		} else {
			
			throw new RuntimeException("Invalid output path: " + outputPath);
			
		}
		
		return l;
		
	}
}
