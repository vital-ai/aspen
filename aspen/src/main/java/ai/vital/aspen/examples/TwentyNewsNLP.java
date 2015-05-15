package ai.vital.aspen.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.steps.MinorThirdStep;
import ai.vital.aspen.groovy.nlp.steps.NamedPersonStep;
import ai.vital.aspen.groovy.nlp.steps.SentenceDetectorStep;
import ai.vital.aspen.groovy.nlp.steps.TextExtractStep;
import ai.vital.aspen.groovy.nlp.steps.WhiteSpaceTokenizerStep;
import ai.vital.vitalsigns.uri.URIGenerator;
import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasEntityInstance;
import ai.vital.domain.Entity;
import ai.vital.domain.EntityInstance;
import ai.vital.vitalservice.model.App;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Container;
import ai.vital.vitalsigns.model.VITAL_Node;

public class TwentyNewsNLP {

	private static TwentyNewsNLP singleton;
	
	private final static Logger log = LoggerFactory.getLogger(TwentyNewsNLP.class);
	
	public static TwentyNewsNLP get(Path optionalConfigPath) throws IOException {
		
		if(singleton == null) {
			
			synchronized(TwentyNewsNLP.class) {

				if(singleton == null) {
					
					log.info("Setting up NLP singleton...");
					
					singleton = new TwentyNewsNLP(optionalConfigPath);
					
				}
				
						
			}
			
		}
		
		return singleton;
		
	}
	
	TextExtractStep extract_step = null;
	SentenceDetectorStep sentdetect_step = null;
	WhiteSpaceTokenizerStep wst_step = null;
	MinorThirdStep m3rd_step = null;
	NamedPersonStep namePersonStep = null;
			
	
	public TwentyNewsNLP(Path optionalConfigPath) throws IOException {
	
		if(optionalConfigPath != null) {
			
			log.info("configuring aspen instance from path: " + optionalConfigPath.toString());
			
			FileSystem fs = FileSystem.get(optionalConfigPath.toUri(), new Configuration());
			
			if(! fs.exists(optionalConfigPath) ) throw new RuntimeException("Config file path not found: " + optionalConfigPath.toString());
			
			FSDataInputStream configStream = null;
			
			try {
				
				configStream = fs.open(optionalConfigPath);
				
				AspenGroovyConfig.get().configure(configStream);
				
				log.info("aspen configured from " + optionalConfigPath.toString());
				
			} finally {
				
				IOUtils.closeQuietly(configStream);
				
				
			}
			
			
		}
		
		log.info("setting up extract step");
		extract_step = new TextExtractStep();
		//don't process html content, we need 1:1 text <-> entities cover
		extract_step.processHtml = false;
		
		log.info("setting up sentence detector step...");
		sentdetect_step = new SentenceDetectorStep();
		sentdetect_step.singleSentencePerBlock = true;
		sentdetect_step.init();
		
		log.info("setting up tokenizer step...");
		wst_step = new WhiteSpaceTokenizerStep(false);
		wst_step.init();

		
		log.info("setting up m3rd step");
		m3rd_step = new MinorThirdStep();
		m3rd_step.init();
		
		log.info("setting up name person step");
		namePersonStep = new NamedPersonStep();
		namePersonStep.init();

		log.info("NLP singleton set up");
		
	}
	
	static App app = new App();
	static {
		app.setID("app");
		app.setOrganizationID("customer");
	}
	
	public synchronized List<GraphObject> process(List<GraphObject> input) throws Exception {
		
		List<GraphObject> output = new ArrayList<GraphObject>(input);
		
		
		for(GraphObject g : input) {
			
			if(!( g instanceof Document)) {
				continue;
			}
			
//			if(type != null && !g.getClass().getSimpleName().equals(type)) {
//				nonType++
//				continue
//			}
			
			String text = g.getProperty("body") != null ? g.getProperty("body").toString() : null;
			
			if(text == null || text.isEmpty()) {
				continue;
			}
			
			
			VitalSigns vs = VitalSigns.get();
		
			//use cache or a container ?
			
			Document originalDocument = (Document)g;				
			
			//empty
			Document d = new Document();
			d.generateURI(app);
			d.setProperty("body", text);
			
			List<GraphObject> list = new ArrayList<GraphObject>();
			
			extract_step.processDocument(d, list);
			
			vs.addToCache(d);
			vs.addToCache(list);
			
			list = new ArrayList<GraphObject>();
			
			sentdetect_step.processDocument(d, list);
			vs.addToCache(list);
			
			list = new ArrayList<GraphObject>();
			
			wst_step.processDocument(d);
		
			/*
			pos_step.processDocument(d)
		
			list = []
			
			chunker_step.processDocument(d, list)
			
			vs.addToCache(list)
			*/
			
			list = new ArrayList<GraphObject>();
			
			m3rd_step.processDocument(d, list);
			
			vs.addToCache(list);
			
			
			list = new ArrayList<GraphObject>();
			namePersonStep.processDocument(d, list);
			vs.addToCache(list);
			
			
			list = new ArrayList<GraphObject>();

			List<VITAL_Node> entities = d.getCollection("entities");
			
			for(VITAL_Node n : entities) {
				
				if(!(n instanceof Entity)) continue;
				
				Entity entity = (Entity)n;
				
				List<VITAL_Node> instances = entity.getCollection("entityInstances");
				
				for( VITAL_Node n2 : instances) {
					
					if(!(n2 instanceof EntityInstance)) continue;
					
					EntityInstance ei = (EntityInstance) n2;
					
					ei.setURI( URIGenerator.generateURI(app, EntityInstance.class) );
					
					Edge_hasEntityInstance edge = new Edge_hasEntityInstance();
					edge.setURI( URIGenerator.generateURI(app, Edge_hasEntityInstance.class) );
					edge.addSource(originalDocument).addDestination(ei);

					output.add(ei);
					output.add(edge);
					
				}
				
				
			}
			
		}
		
		return output;
		
	}
}
