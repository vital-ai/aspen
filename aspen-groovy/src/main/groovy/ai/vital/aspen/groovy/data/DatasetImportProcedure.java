package ai.vital.aspen.groovy.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertBlockToSequenceTask;
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask;
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask;
import ai.vital.aspen.groovy.data.tasks.ResolveURIReferencesTask;
import ai.vital.aspen.groovy.task.AbstractTask;

/**
 * A procedure that imports vital block file into named dataset
 * @author Derek
 *
 */
public class DatasetImportProcedure {

	public List<String> inputPaths;
	
	public String outputDatasetName;
	
	public boolean overwrite = false;
	
	public Map<String, Object> paramsMap;
	
	public DatasetImportProcedure(List<String> inputPaths,
			String outputDatasetName, boolean overwrite,
			Map<String, Object> paramsMap) {
		super();
		this.inputPaths = inputPaths;
		this.outputDatasetName = outputDatasetName;
		this.overwrite = overwrite;
		this.paramsMap = paramsMap;
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
		
		
		if(!overwrite) {
			
			CheckPathTask cpt = new CheckPathTask("name:" + outputDatasetName, paramsMap);
			cpt.mustExist = false;
			cpt.mustnotExist = true;
			cpt.acceptFiles = true;
			cpt.acceptDirectories = true;
			
			l.add(cpt);
			
		} else {
			
			//delete output dataset first
			
			DeletePathTask removeDataset = new DeletePathTask("name:" + outputDatasetName, paramsMap);
			l.add(removeDataset);
		}
		
		String datasetSequencePath = AspenGroovyConfig.get().datasetsLocation + "/" + outputDatasetName + "/" + outputDatasetName + ".vital.seq";
		
		//don't checkout output, always overwrite
		DeletePathTask deleteTask = new DeletePathTask(datasetSequencePath, paramsMap);
		l.add(deleteTask);
		
		
		
		ConvertBlockToSequenceTask convertTask = new ConvertBlockToSequenceTask(inputPaths, datasetSequencePath, paramsMap);
		
		l.add(convertTask);
		
		
		LoadDataSetTask loadDataSetTask = new LoadDataSetTask(paramsMap, datasetSequencePath, outputDatasetName);
		l.add(loadDataSetTask);
		
		l.add(new ResolveURIReferencesTask(paramsMap, outputDatasetName));
		
//		CheckPathTask cpt = new CheckPathTask(outputPath, paramsMap);
//		cpt.mustExist = false;
//		cpt.mustnotExist = !overwrite;
//		cpt.acceptDirectories = false;
//		cpt.validFileExtensions = new String[]{".vital.seq", ".vital.seq.gz"};
		
		
		return l;

	}
	
}
