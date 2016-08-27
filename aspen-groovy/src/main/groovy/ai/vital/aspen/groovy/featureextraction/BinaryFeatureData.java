package ai.vital.aspen.groovy.featureextraction;

import java.util.HashMap;
import java.util.Map;

public class BinaryFeatureData extends FeatureData {

	private static final long serialVersionUID = 1L;

	@Override
	public Map<String, Object> toJSON() {
		return new HashMap<String, Object>();
	}

	@Override
	public void fromJson(Map<String, Object> unwrapped) {
		
	}

}
