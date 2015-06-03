package ai.vital.aspen.groovy.ref;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.modelmanager.AspenPrediction;
import ai.vital.predictmodel.Prediction;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;

/**
 * Sample reference model, used for testing
 * @author Derek
 *
 */
public class ReferenceModel extends AspenModel {

	private static final long serialVersionUID = 1L;

	@Override
	protected boolean acceptResource(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected void processResource(String name, InputStream inputStream)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	protected void onResourcesProcessed() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void validateConfig() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	protected Prediction _predict(VitalBlock input, Map<String, Object> features) {
		return new AspenPrediction(Arrays.asList( input.getMainObject()) );
	}

	@Override
	protected void persistFiles(File tempDir) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isSupervised() {
		return true;
	}

	@Override
	public boolean isCategorical() {
		return true;
	}

}
