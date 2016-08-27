package ai.vital.aspen.groovy.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertBlockToSequenceTask;
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask;
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask;
import ai.vital.aspen.groovy.select.tasks.SelectDataSetTask;
import ai.vital.aspen.groovy.task.AbstractTask;

public class SelectDatasetProcedure {

	public String modelBuilderPath;
	
	public String outputPath;

	//mutually exclusive
	public String queryPath;
	
	//mutually exclusive
	public String urisListPath;

	public Integer limit;
	
	public Integer percent;

	public Integer maxDocs;
	
	public boolean overwrite = false;

	public boolean uriOnlyOutput = false;

	private Map<String, Object> paramsMap;
	
	
	
	public SelectDatasetProcedure(String modelBuilderPath, String outputPath,
			String queryPath, String urisListPath, Integer limit,
			Integer percent, Integer maxDocs, boolean overwrite,
			boolean uriOnlyOutput, Map<String, Object> paramsMap) {
		super();
		this.modelBuilderPath = modelBuilderPath;
		this.outputPath = outputPath;
		this.queryPath = queryPath;
		this.urisListPath = urisListPath;
		this.limit = limit;
		this.percent = percent;
		this.maxDocs = maxDocs;
		this.overwrite = overwrite;
		this.uriOnlyOutput = uriOnlyOutput;
		this.paramsMap = paramsMap;
	}



	public List<AbstractTask> generateTasks() throws Exception {
		
		List<AbstractTask> l = new ArrayList<AbstractTask>();
		
		if(overwrite) {
			
			l.add(new DeletePathTask(outputPath, paramsMap));
			
		} else {

			CheckPathTask cpt = new CheckPathTask(outputPath, paramsMap);
			cpt.acceptDirectories = true;
			cpt.acceptFiles = true;
			cpt.mustExist = false;
			cpt.mustnotExist = true;
			
			l.add(cpt);
			
		}
		
		if( ( queryPath != null && urisListPath != null ) || ( queryPath == null && urisListPath == null)) {
			throw new RuntimeException("queryPath and urisListPath are mutually exclusive, exactly 1 required");
		}
		
		if(queryPath != null) {
			
			CheckPathTask cpt = new CheckPathTask(queryPath, paramsMap);
			cpt.acceptFiles = true;
			cpt.acceptDirectories = false;
			cpt.mustExist = true;
			cpt.mustnotExist = false;

			l.add(cpt);
			
		}
		
		/* XXX consider further task split
		if(urisListPath != null) {
			
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
		}
		
		*/
		
		
		l.add(new SelectDataSetTask(paramsMap, modelBuilderPath, outputPath, queryPath, urisListPath, limit, percent, maxDocs, overwrite, uriOnlyOutput));
		
		return l;
		
	}
	
}
