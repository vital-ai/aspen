package ai.vital.aspen.groovy.predict.tasks;

import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.NumericalFeature;

public class CollectNumericalFeatureDataTask implements ModelTrainingTask {

	public NumericalFeature feature;
	
	public String datasetName;
	
	public NumericalFeatureData results;

	
	public CollectNumericalFeatureDataTask(NumericalFeature f, String datasetName) {
		this.feature = f;
		this.datasetName = datasetName;
	}

	@Override
	public void validateResult() {
		
		if(feature == null) throw new NullPointerException("No feature set");
		
		if(datasetName == null) throw new NullPointerException("No datasetName set");
		
		if(results == null) throw new NullPointerException("No numerical feature data: " + feature.getName());
		
	}
	
	
}
