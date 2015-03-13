package ai.vital.aspen.groovy.nlp.steps

import opennlp.tools.sentdetect.SentenceDetectorME;

import org.slf4j.Logger;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.step.AbstractStep
import ai.vital.domain.Document;
import ai.vital.domain.TextBlock;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import opennlp.tools.sentdetect.SentenceDetectorME;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasSentence;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.models.SentenceDetectorModel;


import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.aspen.groovy.ontology.VitalOntology



class SentenceDetectorStep extends AbstractStep {
	
	
	public final static String  SENTENCEDETECTOR_VS = "sentencedetector_vs";
	
	private final static Logger log = LoggerFactory.getLogger(SentenceDetectorStep.class);
	
	private SentenceDetectorME detector;
	
	//this is a config option - very handy
	public boolean singleSentencePerBlock = false
	
	public void init()  {
		
		try {
			detector = SentenceDetectorModel.getDetector()
		} catch(Exception e){}
		
		if(detector == null) {
			
			InputStream inputStream = null
			
			try {
				
				if( AspenGroovyConfig.get().loadResourcesFromClasspath ) {
					
					String path = "/resources/models/en-sent.bin"
					
					log.info("Initializing sentences model from path: {}", path);
					
					inputStream = AspenGroovyConfig.class.getResourceAsStream(path)
					
					if(inputStream == null) throw new Exception("Model not found: ${path}")
					
				} else {
				
					String resDir = AspenGroovyConfig.get().resourcesDir
					if(!resDir) throw new RuntimeException("resourcesDir not set")
				
					File modelFile = new File(new File(resDir, "models"), "en-sent.bin");
				
					log.info("Initializing sentences model from file: {}", modelFile.getAbsolutePath());
				
					if(!modelFile.exists()) throw new Exception("Model not found: ${modelFile.absolutePath}")
				
					inputStream = new FileInputStream(modelFile)
						
				}
				
				SentenceDetectorModel.init(inputStream);
				
				detector = SentenceDetectorModel.getDetector()
				
			} finally {
				IOUtils.closeQuietly(inputStream)
			}
			
		}
		
	}

	
	public String getName() {
		return SENTENCEDETECTOR_VS;
	}

	
	public void processDocument(Document doc, List results) {

		String uri = doc.getURI();
			
		log.info("Processing doc: {}", uri);

		int startIndex = 1;
			
		for( TextBlock block : doc.getTextBlocks() ) {
			
			startIndex = processTextBloc(results, doc, block, startIndex, singleSentencePerBlock);

		}
		
	}

	public int processTextBloc(List results, Document doc, TextBlock block, int index, boolean singleSentencePerBlock) {
		
		String text = block.text;
		
		String[] sentArray = null;
		
		if(singleSentencePerBlock) {
			sentArray = [text];
		} else {
			sentArray = detector.sentDetect(text);
		}
		
		int low_index = 0;
		
		int high_index = 0;
		
		int cursor = 0;
		
		List<Sentence> sentences = new ArrayList<Sentence>();
		
		for(String s : sentArray ) {
			
			//whitespace bug
			s = s.trim();
				
			low_index = text.indexOf(s, cursor);
				
			if(low_index < 0) {
				throw new RuntimeException("Sentence not found! The sentence detector must have changed the sentence.");
			}

			high_index = low_index + ( s.length() );

			Sentence sentObj = new Sentence();
			
			sentObj.startPosition = low_index;
			sentObj.endPosition = high_index;
			sentObj.sentenceNumber = index;
			sentObj.setURI(doc.getURI() + "#sentence_" + index++);
			
			sentences.add(sentObj);
			
			cursor = high_index;
			
		}
		
//		block.setSentences(sentences);
		results.addAll(sentences);
		results.addAll(EdgeUtils.createEdges(block, sentences, Edge_hasSentence, VitalOntology.Edge_hasSentenceURIBase));
		
		return index;
		
	}
	
	
}
