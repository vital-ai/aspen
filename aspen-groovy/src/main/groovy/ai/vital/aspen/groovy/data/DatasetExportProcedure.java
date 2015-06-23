package ai.vital.aspen.groovy.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertSequenceToBlockTask;
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask;
import ai.vital.aspen.groovy.data.tasks.SaveDataSetTask;
import ai.vital.aspen.groovy.task.AbstractTask;

/**
 * A procedure that imports vital block file into named dataset
 * @author Derek
 *
 */
public class DatasetExportProcedure {

	public String inputDatasetName;
	
	public String outputPath;
	
	public boolean overwrite = false;
	
	public Map<String, Object> paramsMap;
	
	public DatasetExportProcedure(String inputDatasetName,
			String outputPath, boolean overwrite,
			Map<String, Object> paramsMap) {
		super();
		this.inputDatasetName = inputDatasetName;
		this.outputPath = outputPath;
		this.overwrite = overwrite;
		this.paramsMap = paramsMap;
	}

	
	public List<AbstractTask> generateTasks() {
		
		List<AbstractTask> l = new ArrayList<AbstractTask>();
		
		CheckPathTask cpt = new CheckPathTask("name:" + inputDatasetName, paramsMap);
		
		cpt.acceptDirectories = true;
		cpt.acceptFiles = true;
		cpt.mustExist = true;
		cpt.mustnotExist = false;
		l.add(cpt);
			
		
		//dump to sequence file first
		String datasetSequencePath = AspenGroovyConfig.get().datesetsLocation + "/" + inputDatasetName + "/" + inputDatasetName + ".vital.seq";

		
		//check output dataset name
		
		CheckPathTask cpt2 = new CheckPathTask(outputPath, paramsMap);
		cpt.mustnotExist = !overwrite;
		cpt.acceptFiles = true;
		cpt.acceptDirectories = true;
		cpt.validFileExtensions = new String[]{".vital.gz", ".vital"};
		l.add(cpt);
		
		if(!overwrite) {
			
			
		} else {
			
			//delete output block first
			DeletePathTask removeDataset = new DeletePathTask(outputPath, paramsMap);
			l.add(removeDataset);
		}
		
		//delete saved dataset and dump again
		l.add(new DeletePathTask(datasetSequencePath, paramsMap));
		
		SaveDataSetTask sdst = new SaveDataSetTask(paramsMap, inputDatasetName, datasetSequencePath);
		l.add(sdst);

		
		l.add(new ConvertSequenceToBlockTask(Arrays.asList(datasetSequencePath), outputPath, paramsMap )); 
		
//		CheckPathTask cpt = new CheckPathTask(outputPath, paramsMap);
//		cpt.mustExist = false;
//		cpt.mustnotExist = !overwrite;
//		cpt.acceptDirectories = false;
//		cpt.validFileExtensions = new String[]{".vital.seq", ".vital.seq.gz"};
		
		
		return l;

	}
	
}
