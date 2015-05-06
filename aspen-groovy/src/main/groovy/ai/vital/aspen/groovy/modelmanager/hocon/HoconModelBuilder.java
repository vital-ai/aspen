package ai.vital.aspen.groovy.modelmanager.hocon;

import java.util.ArrayList;
import java.util.List;

import ai.vital.aspen.groovy.modelmanager.domain.Feature;
import ai.vital.aspen.groovy.modelmanager.domain.Model;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class HoconModelBuilder {

	public static Model parseConfigString(String input) throws Exception {
		
		Config cfg = ConfigFactory.parseString(input);
		
		Model m = new Model();
		
		Config modelCfg = cfg.getConfig("Model");
		
		m.setName(modelCfg.getString("name"));
		m.setType(modelCfg.getString("type"));
		m.setURI(modelCfg.getString("URI"));
		
//		m.setAlgorithm(algorithm);
		
//		Config config = cfg.getConfig("Feature");
		
		List<? extends Config> featuresCfgs = cfg.getConfigList("Features");
		
		List<Feature> features = new ArrayList<Feature>();
		
		for(Config featureCfg : featuresCfgs) {
			
			Feature f = new Feature();
			f.setName(featureCfg.getString("name"));
			f.setType(featureCfg.getString("type"));
			f.setURI(featureCfg.getString("URI"));
			f.setValue(featureCfg.getString("value"));
			
			features.add(f);
			
		}
		
		m.setFeatures(features);
		
		return m;
	}
	
}
