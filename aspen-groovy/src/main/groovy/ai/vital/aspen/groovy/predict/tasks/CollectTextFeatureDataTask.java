package ai.vital.aspen.groovy.predict.tasks;

import ai.vital.aspen.groovy.featureextraction.TextFeatureData;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.TextFeature;

public class CollectTextFeatureDataTask implements ModelTrainingTask {

	public TextFeature feature;
	
	public TextFeatureData results;
	

	public CollectTextFeatureDataTask(TextFeature f) {
		this.feature = f;
	}

	@Override
	public void validateResult() {
		
		if(feature == null) throw new NullPointerException("No feature set");
		
		if(results == null) throw new NullPointerException("No text feature data: " + feature.getName());
		
	}
	
	
}
