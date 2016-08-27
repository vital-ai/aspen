package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.CategoricalFeature;
import ai.vital.predictmodel.Taxonomy;

public class CollectCategoricalFeatureTaxonomyDataTask extends ModelTrainingTask {

	public final static String CATEGORICAL_FEATURE_DATA_SUFFIX = "-categorical-feature-data";
	
	public CategoricalFeature categoricalFeature;
	
	public String datasetName;

	public Taxonomy taxonomy;

	public CollectCategoricalFeatureTaxonomyDataTask(AspenModel model, Taxonomy taxonomy, Map<String, Object> paramsMap, CategoricalFeature categoricalFeature, String datasetName) {
		super(model, paramsMap);
		this.taxonomy = taxonomy;
		this.categoricalFeature = categoricalFeature;
		this.datasetName = datasetName;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(categoricalFeature.getName() + CATEGORICAL_FEATURE_DATA_SUFFIX);
	}
	

	

}
