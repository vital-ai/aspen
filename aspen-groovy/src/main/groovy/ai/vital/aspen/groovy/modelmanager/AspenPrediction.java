package ai.vital.aspen.groovy.modelmanager;

import java.util.List;

import ai.vital.predictmodel.Prediction;
import ai.vital.vitalsigns.model.GraphObject;

/**
 * simple pass-through predictions result
 * @author Derek
 *
 */
public class AspenPrediction extends Prediction {

	List<GraphObject> objects;

	public AspenPrediction(List<GraphObject> objects) {
		super();
		this.objects = objects;
	}

	public List<GraphObject> getObjects() {
		return objects;
	}

	public void setObjects(List<GraphObject> objects) {
		this.objects = objects;
	}
	
	public String getCategory() {
		return "not set!";
	}
	
	
	
}
