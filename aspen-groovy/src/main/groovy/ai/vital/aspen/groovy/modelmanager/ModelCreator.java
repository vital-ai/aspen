package ai.vital.aspen.groovy.modelmanager;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


//import ai.vital.aspen.groovy.modelmanager.builder.ModelBuilder;
import ai.vital.aspen.groovy.modelmanager.domain.Model;
import ai.vital.aspen.groovy.modelmanager.hocon.HoconModelBuilder;

public class ModelCreator {

	protected Map<String, Class<? extends AspenModel>> type2ModelClass = new HashMap<String, Class<? extends AspenModel>>();

	
	public ModelCreator(Map<String, Class<? extends AspenModel>> type2ModelClass) {
		this.type2ModelClass = type2ModelClass;
	}


	//creates an instance of a model but does not load the details
	public AspenModel createModel(byte[] builderFileContent) throws Exception {
		
		String builderCode = new String(builderFileContent, StandardCharsets.UTF_8);
		
//		Model modelEl = ModelBuilder.fromModelString(builderCode);
		
		Model modelEl = HoconModelBuilder.parseConfigString(builderCode);
		
		String type = modelEl.getType();
		
		if(type == null || type.isEmpty()) throw new Exception("Null or empty  model type property");

		String name = modelEl.getName();
		if(name == null || name.isEmpty()) throw new Exception("Null or empty model name");
		
		String uri = modelEl.getURI();
		if(uri == null || uri.isEmpty()) throw new Exception("Null or empty model URI");
		
		AspenModel m = null;
		
		Class<? extends AspenModel> class1 = type2ModelClass.get(type);
		
		if(class1 == null) throw new Exception("No model class for type " + type + " found");
		
		m = class1.newInstance();
		
		m.setModelElement(modelEl);
		
		return m;
		
		
//		Config cfg = ConfigFactory.parseReader(new InputStreamReader(builderFileInputStream, StandardCharsets.UTF_8));
//		
//		Config config = cfg.getConfig("MODEL");
		
		//parse
		
	}
	
}
