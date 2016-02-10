package ai.vital.aspen.groovy.predict;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertBlockToSequenceTask;
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask;
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask;
import ai.vital.aspen.groovy.predict.tasks.FeatureQueryTask;
import ai.vital.aspen.groovy.predict.tasks.LoadModelTask;
import ai.vital.aspen.groovy.predict.tasks.TestModelIndependentTask;
import ai.vital.aspen.groovy.task.AbstractTask;

public class ModelTestingProcedure {

	public String inputPath;
	
	public Map<String, Object> paramsMap;

	private String modelPath; 
	
	protected String inputDatasetName = "training-input-dataset";
	
	public ModelTestingProcedure(String inputPath, String modelPath, Map<String, Object> paramsMap) {
		super();
		this.inputPath = inputPath;
		this.modelPath = modelPath;
		this.paramsMap = paramsMap;
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
		
		
		if(inputPath.startsWith("name:")) {
			
			inputDatasetName = inputPath.substring(5);
			
			CheckPathTask cpt = new CheckPathTask(inputPath, paramsMap);
			
			cpt.acceptDirectories = true;
			cpt.acceptFiles = true;
			cpt.mustExist = true;
			cpt.mustnotExist = false;
			l.add(cpt);
			
			l.add(new LoadModelTask(paramsMap, modelPath));
			
			LoadDataSetTask loadDataSetTask = new LoadDataSetTask(paramsMap, inputPath, inputDatasetName);
			l.add(loadDataSetTask);
			
		} else if(inputPath.startsWith("spark-segment:")) {
			
			CheckPathTask cpt = new CheckPathTask(inputPath, paramsMap);
			cpt.acceptDirectories = true;
			cpt.acceptFiles = true;
			cpt.mustExist = true;
			cpt.mustnotExist = false;
			l.add(cpt);
			
			l.add(new LoadModelTask(paramsMap, modelPath));
			
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
			
			l.add(new LoadModelTask(paramsMap, modelPath));
			
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
			
			l.add(new LoadModelTask(paramsMap, modelPath));
			
			String datasetSequencePath = inputPath;
			
			LoadDataSetTask loadDataSetTask = new LoadDataSetTask(paramsMap, datasetSequencePath, inputDatasetName);
			l.add(loadDataSetTask);
			
			
		} else {
			throw new RuntimeException("Expected input to be either a named RDD, vital seq or block file");
		}
		
		l.add(new FeatureQueryTask(paramsMap, inputDatasetName, modelPath));
		
		l.add(new TestModelIndependentTask(paramsMap, modelPath, inputDatasetName));
		
		return l;
	}
	
}
