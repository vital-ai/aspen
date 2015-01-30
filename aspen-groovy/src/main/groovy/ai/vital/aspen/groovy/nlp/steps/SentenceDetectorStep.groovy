package ai.vital.aspen.groovy.nlp.steps

import opennlp.tools.sentdetect.SentenceDetectorME;

import org.slf4j.Logger;

import ai.vital.aspen.groovy.step.AbstractStep
import ai.vital.domain.Document;
import ai.vital.domain.TextBlock;
import ai.vital.vitalsigns.model.container.Payload;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import opennlp.tools.sentdetect.SentenceDetectorME;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasSentence;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;

import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.models.SentenceDetectorModel;
import ai.vital.vitalsigns.model.container.Payload;


import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.aspen.groovy.ontology.VitalOntology



class SentenceDetectorStep extends AbstractStep {
	
	
	public final static String  SENTENCEDETECTOR_VS = "sentencedetector_vs";
	
	private final static Logger log = LoggerFactory.getLogger(SentenceDetectorStep.class);
	
	private SentenceDetectorME detector;
	
	
	public void init()  {
		
				
		File modelFile = new File("resources/models", "en-sent.bin");
		
		log.info("Initializing sentences model from file: {} ...", modelFile.getAbsolutePath());
		
		SentenceDetectorModel.init(modelFile);
		
		long start = System.currentTimeMillis();
		
		detector = SentenceDetectorModel.getDetector();
		
		long stop = System.currentTimeMillis();
	
		log.info("Sentences detector obtained, {}ms", (stop-start));
		
	}

	
	public String getName() {
		return SENTENCEDETECTOR_VS;
	}

	
	public void processDocument(Document doc, List results) {

		
		boolean singleSentencePerBlock = false
		
		
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
