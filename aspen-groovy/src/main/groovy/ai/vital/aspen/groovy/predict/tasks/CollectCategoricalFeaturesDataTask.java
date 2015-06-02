package ai.vital.aspen.groovy.predict.tasks;

import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.CategoricalFeature;

public class CollectCategoricalFeaturesDataTask implements ModelTrainingTask {

	public CategoricalFeature feature;
	
	public CategoricalFeatureData results;

	public CollectCategoricalFeaturesDataTask(CategoricalFeature f) {
		this.feature = f;
	}

	@Override
	public void validateResult() {
		
		if(feature == null) throw new NullPointerException("No feature set");
		
		if(results == null) throw new NullPointerException("No categorical feature data: " + feature.getName());
		
	}

}
