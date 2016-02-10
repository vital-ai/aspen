package ai.vital.aspen.groovy.predict;

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
import ai.vital.aspen.groovy.predict.tasks.FeatureQueryTask;
import ai.vital.aspen.groovy.predict.tasks.LoadModelTask;
import ai.vital.aspen.groovy.predict.tasks.ModelPredictTask;
import ai.vital.aspen.groovy.task.AbstractTask;

public class ModelPredictProcedure {

	public String inputPath;
	
	public String modelPath;
	
	public String outputPath;
	
	public boolean overwrite = false;
	
	public Map<String, Object> paramsMap;
	
	public String inputDatasetName = "model-predict-input";
	
	public String outputDatesetName = "model-predict-output";
	
	public ModelPredictProcedure(String inputPath, String modelPath,
			String outputPath, boolean overwrite, Map<String, Object> paramsMap) {
		super();
		this.inputPath = inputPath;
		this.modelPath = modelPath;
		this.outputPath = outputPath;
		this.paramsMap = paramsMap;
		this.overwrite = overwrite;
	}



	public List<AbstractTask> generateTasks() throws Exception {
		
		List<AbstractTask> l = new ArrayList<AbstractTask>();
		
		//check model exists
		
		CheckPathTask cmp = new CheckPathTask(modelPath, paramsMap);
		cmp.mustExist = true;
		cmp.mustnotExist = false;
		cmp.acceptFiles = true;
		cmp.acceptDirectories = true;
		
		l.add(cmp);
		
		
		if(overwrite) {
			
			l.add(new DeletePathTask(outputPath, paramsMap));
			
		} else {
			
			CheckPathTask cpt = new CheckPathTask(outputPath, paramsMap);
			cpt.mustExist = false;
			cpt.mustnotExist = true;
			cpt.acceptDirectories = true;
			cpt.acceptFiles = true;
			l.add(cpt);
			
		}
		
		

		if(inputPath.startsWith("name:")) {
			
			inputDatasetName = inputPath.substring(5);
			
			CheckPathTask cpt = new CheckPathTask(inputPath, paramsMap);
			
			cpt.acceptDirectories = true;
			cpt.acceptFiles = true;
			cpt.mustExist = true;
			cpt.mustnotExist = false;
			l.add(cpt);
			
			LoadDataSetTask loadDataSetTask = new LoadDataSetTask(paramsMap, inputPath, inputDatasetName);
			l.add(loadDataSetTask);
			
		} else if(inputPath.startsWith("spark-segment:")) {

			CheckPathTask cpt = new CheckPathTask(inputPath, paramsMap);
			
			cpt.acceptDirectories = true;
			cpt.acceptFiles = true;
			cpt.mustExist = true;
			cpt.mustnotExist = false;
			l.add(cpt);
			
			LoadDataSetTask loadDataSetTask = new LoadDataSetTask(paramsMap, inputPath, inputDatasetName);
			l.add(loadDataSetTask);
			
		} else if(inputPath.endsWith(".vital") || inputPath.endsWith(".vital.gz")) {
			
			CheckPathTask cpt = new CheckPathTask(inputPath, paramsMap);
			
			cpt.acceptDirectories = true;
			cpt.acceptFiles = true;
			cpt.mustExist = true;
			cpt.mustnotExist = false;
			cpt.validFileExtensions = new String[]{".vital", ".vital.gz"};
			l.add(cpt);
			
			String datasetSequencePath = AspenGroovyConfig.get().datesetsLocation + "/" + inputDatasetName + "/" + inputDatasetName + ".vital.seq";
			
			//don't checkout output, always overwrite
			DeletePathTask deleteTask = new DeletePathTask(datasetSequencePath, paramsMap);
			l.add(deleteTask);
			
			
			
			ConvertBlockToSequenceTask convertTask = new ConvertBlockToSequenceTask(Arrays.asList(inputPath), datasetSequencePath, paramsMap);
			
			l.add(convertTask);
			
			
			LoadDataSetTask loadDataSetTask = new LoadDataSetTask(paramsMap, datasetSequencePath, inputDatasetName);
			l.add(loadDataSetTask);
			
		} else if(inputPath.endsWith(".vital.seq")){
			
			CheckPathTask cpt = new CheckPathTask(inputPath, paramsMap);
			
			cpt.acceptDirectories = true;
			cpt.acceptFiles = true;
			cpt.mustExist = true;
			cpt.mustnotExist = false;
			cpt.validFileExtensions = new String[]{".vital.seq"};
			l.add(cpt);
			
			String datasetSequencePath = inputPath;
			
			LoadDataSetTask loadDataSetTask = new LoadDataSetTask(paramsMap, datasetSequencePath, inputDatasetName);
			l.add(loadDataSetTask);
			
			
		} else {
			throw new RuntimeException("Expected input to be either a named RDD, vital seq or block file");
		}
		
		
		l.add(new LoadModelTask(paramsMap, modelPath));
		
		l.add(new FeatureQueryTask(paramsMap, inputDatasetName, modelPath));
		
		if(outputPath.startsWith("name:")) {
			
			outputDatesetName = outputPath.substring(5);
			
			l.add(new ModelPredictTask(paramsMap, inputDatasetName, outputDatesetName, modelPath));
			
		} else if(outputPath.endsWith(".vital") || outputPath.endsWith(".vital.gz")) {
			
			String datasetSequencePath = AspenGroovyConfig.get().datesetsLocation + "/" + outputDatesetName + "/" + outputDatesetName + ".vital.seq";
			
			//don't checkout output, always overwrite
			DeletePathTask deleteTask = new DeletePathTask(datasetSequencePath, paramsMap);
			l.add(deleteTask);
			
			
			l.add(new ModelPredictTask(paramsMap, inputDatasetName, outputDatesetName, modelPath));
			
			SaveDataSetTask sdst = new SaveDataSetTask(paramsMap, outputDatesetName, datasetSequencePath);
			l.add(sdst);
			
			
			ConvertSequenceToBlockTask convertTask = new ConvertSequenceToBlockTask(Arrays.asList(datasetSequencePath), outputPath, paramsMap);
			
			l.add(convertTask);
			
		} else if(outputPath.endsWith(".vital.seq")){
			
			l.add(new ModelPredictTask(paramsMap, inputDatasetName, outputDatesetName, modelPath));
			
			SaveDataSetTask sdst = new SaveDataSetTask(paramsMap, outputDatesetName, outputPath);
			l.add(sdst);
			
		} else {
			throw new RuntimeException("Expected input to be either a named RDD, vital seq or block file");
		}
		
		
		
		return l;
	}
}
