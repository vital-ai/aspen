package ai.vital.aspen.groovy.predict.tasks;

import java.util.Map;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

/**
 * Implementation must return the training set documents count
 * @author Derek
 *
 */
public class CountDatasetTask extends ModelTrainingTask {

	public static final String DOCS_COUNT_SUFFIX = "-docs-count";

	public String datasetName;
	
	public Integer result = null;
	
	public CountDatasetTask(Map<String, Object> paramsMap, String datasetName) {
		super(paramsMap);
		this.datasetName = datasetName;
	}


	
	@Override
	public void checkDepenedencies() {
		
		if(datasetName == null) throw new RuntimeException("No datasetName set");
		
	}



	@Override
	public void onTaskComplete() {

		CountDatasetTask ctst = this;
		if(ctst.result == null || ctst.result < 0) throw new RuntimeException("Documents count must not be lesser than 0!");
		if(ctst.result < 2) throw new RuntimeException("Cannot progress, training documents count is lesser than 2");		
		
		paramsMap.put(datasetName + DOCS_COUNT_SUFFIX, result);
		
		
	}

}
