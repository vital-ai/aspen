package ai.vital.aspen.groovy.featureextraction;

import java.io.Serializable;
import java.util.Map;

/**
 * Container for models
 * @author Derek
 *
 */
public abstract class FeatureData implements Serializable {

	private static final long serialVersionUID = 1L;

	public abstract Map<String, Object> toJSON();

	public abstract void fromJson(Map<String, Object> unwrapped);

}
