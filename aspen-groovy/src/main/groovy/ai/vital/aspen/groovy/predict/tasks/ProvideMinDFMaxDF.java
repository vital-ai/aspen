package ai.vital.aspen.groovy.predict.tasks;

import java.util.Map;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class ProvideMinDFMaxDF extends ModelTrainingTask {

	public final static String MIN_DF_SUFFIX = "-min-df";
	
	public final static String MAX_DF_SUFFIX = "-max-df";
	
	public String datasetName;

	public Integer docsCount;
	
	public Integer minDF;
	
	public Integer maxDF;
	
	public ProvideMinDFMaxDF(Map<String, Object> paramsMap, String datasetName, Integer docsCount) {
		super(paramsMap);
		this.datasetName = datasetName;
		this.docsCount = docsCount;
	}

	
	@Override
	public void checkDepenedencies() {
		
		if(datasetName == null) throw new NullPointerException("No datasetName set");
		
		docsCount = (Integer) paramsMap.get(datasetName + CountDatasetTask.DOCS_COUNT_SUFFIX);
		
		if(docsCount == null) throw new NullPointerException("No docs count found for dataset: " + datasetName);
		 
	}

	@Override
	public void onTaskComplete() {
		
		if(minDF == null) throw new NullPointerException("No minDF");
		
		if(maxDF == null) throw new NullPointerException("No maxDF");

		paramsMap.put(datasetName + MIN_DF_SUFFIX, minDF);
		paramsMap.put(datasetName + MAX_DF_SUFFIX, maxDF);
		
	}


}
