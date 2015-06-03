package ai.vital.aspen.groovy.nlp.models;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.util.model.BaseModel;
import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.predictmodel.Prediction;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;

public class OpenNLPModel extends AspenModel {

	private static final long serialVersionUID = 1L;

	private BaseModel baseModel;
	
	public OpenNLPModel() {}
	
	public final static String chunker = "opennlp-chunker";
	
	public final static String ner_person = "opennlp-ner-person";
	
	public final static String pos = "opennlp-pos";
	
	public final static String sentences = "opennlp-sentences";
	
	public final static String sentiment = "opennlp-sentiment";
	
	public final static String tokenizer = "opennlp-tokenizer";
	
	public final static Set<String> validTypes = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
		chunker,
		ner_person,
		pos,
		sentences, 
		sentiment,
		tokenizer
	)));
	
	@Override
	protected boolean acceptResource(String name) {
		
		String t = getType();
		
		if(t.equals(chunker)) {
			
			return name.endsWith("-chunker.bin");
			
			
		} else {
			throw new RuntimeException("Unhandled opennlp model type: " + t);
		}
		
	}

	@Override
	protected void processResource(String name, InputStream inputStream) throws Exception {

		if( name.endsWith("-chunker.bin") ) {
			this.baseModel = new ChunkerModel(inputStream);
		}
	}

	@Override
	protected void onResourcesProcessed() throws Exception {
		
		if(baseModel == null) throw new Exception("No base model set up!");
		
	}
	
	@Override
	public void validateConfig() throws Exception {

		if(!validTypes.contains(getType())) throw new RuntimeException("Unhandled opennlp model type: " + getType());
		
	}

	public BaseModel getBaseModel() {
		return baseModel;
	}

	@Override
	protected Prediction _predict(VitalBlock input, Map<String, Object> features) {
		// TODO Auto-generated method stub
		return null;
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
