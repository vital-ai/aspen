package ai.vital.aspen.groovy.predict.tasks;

import ai.vital.aspen.groovy.featureextraction.TextFeatureData;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.TextFeature;

public class CollectTextFeatureDataTask implements ModelTrainingTask {

	public TextFeature feature;
	
	public String datasetName;
	
	public Integer minDF;
	
	public Integer maxDF;
	
	
	public TextFeatureData results;
	

	public CollectTextFeatureDataTask(TextFeature f, String datasetName, Integer minDF, Integer maxDF) {
		this.feature = f;
		this.datasetName = datasetName;
		this.minDF = minDF;
		this.maxDF = maxDF;
	}

	@Override
	public void validateResult() {
		
		if(feature == null) throw new NullPointerException("No feature set");
		
		if(datasetName == null) throw new NullPointerException("No datasetName set");
		
		if(minDF == null) throw new NullPointerException("No minDF set");
		
		if(maxDF == null) throw new NullPointerException("No maxDF set");
		
		if(results == null) throw new NullPointerException("No text feature data: " + feature.getName());
		
	}
	
	
}
