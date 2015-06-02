package ai.vital.aspen.groovy.predict.tasks;

import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.NumericalFeature;

public class CollectNumericalFeatureDataTask implements ModelTrainingTask {

	NumericalFeature feature;
	
	NumericalFeatureData results;

	public CollectNumericalFeatureDataTask(NumericalFeature f) {
		this.feature = f;
	}

	@Override
	public void validateResult() {
		
		if(feature == null) throw new NullPointerException("No feature set");
		
		if(results == null) throw new NullPointerException("No numerical feature data: " + feature.getName());
		
	}
	
	
}
