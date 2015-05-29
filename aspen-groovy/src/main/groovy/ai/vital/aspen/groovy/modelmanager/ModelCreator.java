package ai.vital.aspen.groovy.modelmanager;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.SerializationUtils;

import ai.vital.aspen.groovy.featureextraction.PredictionModelAnalyzer;
import ai.vital.predictmodel.PredictionModel;
import ai.vital.predictmodel.builder.ModelBuilder;
import ai.vital.predictmodel.builder.ModelString;
import ai.vital.predictmodel.builder.ToModelImpl;
//import ai.vital.aspen.groovy.modelmanager.builder.ModelBuilder;

public class ModelCreator {

	static ModelBuilder modelBuilder = new ModelBuilder();
	
	protected Map<String, Class<? extends AspenModel>> type2ModelClass = new HashMap<String, Class<? extends AspenModel>>();

	
	public ModelCreator(Map<String, Class<? extends AspenModel>> type2ModelClass) {
		this.type2ModelClass = type2ModelClass;
	}


	//creates an instance of a model but does not load the details
	public AspenModel createModel(byte[] builderFileContent) throws Exception {
		
		String builderCode = new String(builderFileContent, StandardCharsets.UTF_8);
		
//		Model modelEl = ModelBuilder.fromModelString(builderCode);
		
//		Model modelEl = HoconModelBuilder.parseConfigString(builderCode);
		
		ModelString modelString = new ModelString();
		modelString.setModelString(builderCode);
		
		PredictionModel modelEl = new ToModelImpl().toModel(modelString.toModel());
		
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
		
		m.setBuilderContent(builderCode);
		
		PredictionModelAnalyzer.fixFunctionsAggregatesOrder(modelEl);
		
		return m;
		
		
//		Config cfg = ConfigFactory.parseReader(new InputStreamReader(builderFileInputStream, StandardCharsets.UTF_8));
//		
//		Config config = cfg.getConfig("MODEL");
		
		//parse
		
	}


	public AspenModel createModelFromObject(byte[] objectFileContent) throws Exception {
		
		AspenModel modelEl = SerializationUtils.deserialize(objectFileContent);
		
		String type = modelEl.getType();
		
		if(type == null || type.isEmpty()) throw new Exception("Null or empty  model type property");

		String name = modelEl.getName();
		if(name == null || name.isEmpty()) throw new Exception("Null or empty model name");
		
		String uri = modelEl.getURI();
		if(uri == null || uri.isEmpty()) throw new Exception("Null or empty model URI");
		
		return modelEl;
	}
	
}
