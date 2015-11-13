package ai.vital.aspen.groovy.nlp.steps

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.step.AbstractStep

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import opennlp.tools.chunker.ChunkerME;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vitalai.domain.nlp.Document;
import com.vitalai.domain.nlp.Edge_hasNounPhrase;
import com.vitalai.domain.nlp.Edge_hasVerbPhrase;
import com.vitalai.domain.nlp.NounPhrase;
import com.vitalai.domain.nlp.PosTag;
import com.vitalai.domain.nlp.Sentence;
import com.vitalai.domain.nlp.TextBlock;
import com.vitalai.domain.nlp.Token;
import com.vitalai.domain.nlp.VerbPhrase;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.model.PosTagsUtils;
import ai.vital.aspen.groovy.nlp.model.TokenUtils;
import ai.vital.aspen.groovy.nlp.models.ChunkerModelWrapper;
import ai.vital.aspen.groovy.ontology.VitalOntology

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.vitalsigns.global.GlobalHashTable;


class ChunkerStep extends AbstractStep {

	public final static String CHUNKER_VS = "chunker_vs";
	
	private final static Logger log = LoggerFactory.getLogger(ChunkerStep.class);
	
	private ChunkerME chunker;
	
	
	public void init()  {

		try {
			chunker = ChunkerModelWrapper.getChunker();
		} catch(Exception e) {
		}	
		

		if(chunker == null) {
			
			long start = System.currentTimeMillis();
			
			InputStream inputStream = null
					
			try {
						
				if(AspenGroovyConfig.get().loadResourcesFromClasspath) {
							
					String path = "/resources/models/en-chunker.bin"
									
					log.info("Initializing chunker model from classpath path: {}", path)
					
					inputStream = AspenGroovyConfig.class.getResourceAsStream(path)				
					
					if(inputStream == null) throw new RuntimeException("Model file not found in classpath: ${path}")
					
				} else {
				
					String resDir = AspenGroovyConfig.get().resourcesDir
					if(!resDir) throw new RuntimeException("resourcesDir not set")			
				
					File modelFile = new File(new File(resDir, "models"), "en-chunker.bin");
					
					if(!modelFile.exists()) throw new RuntimeException("Mode file not found: ${modelFile.absolutePath}")		
					
					log.info("Initializing Chunker model from file: {}", modelFile.getAbsolutePath());
				
					inputStream = new FileInputStream(modelFile)			
				}
						
				ChunkerModelWrapper.init(inputStream);
						
				chunker = ChunkerModelWrapper.getChunker();
						
				long stop = System.currentTimeMillis();
						
				log.info("Chunker model obtained, {}ms", stop - start);
						
			} finally {
						
				IOUtils.closeQuietly(inputStream)
				
			}
			
		}
				
	}
	
	
	public String getName() {
		return CHUNKER_VS;
	}

	
	public void processDocument(Document doc, List results) {
			
			String docUri = doc.getURI();
			
			log.debug("Processing document {} ...", docUri);
			int tbindex = 1;
			
			List tbs = doc.getTextBlocks();
			
			for(TextBlock b : tbs) {
				
				long start = System.currentTimeMillis();
				List sentences = b.getSentences();
				long stop = System.currentTimeMillis();
				
				log.info("Processing tb: " + tbindex++ + " of " + tbs.size() + " with " + sentences.size() + " sentences, " + (stop-start) + "ms - HASH SIZE: " + GlobalHashTable.get().size());
				
				int sentenceIndex = 1;
				for(Sentence s : sentences) {
					
					processSentence(results, docUri, s);
					
				}
				
			}
		
		}
		
	

	public void processSentence(List results, String docUri, Sentence s) {
		
		List<Token> tokensList = TokenUtils.getTokens(s);// s.getTokens();
		
		List<PosTag> posTags = PosTagsUtils.getPosTags(s);// s.getPosTags();
		
		String[] tags = new String[tokensList.size()];
		
		String[] tokens = new String[tokensList.size()];
		
		for( int i = 0 ; i < tokensList.size(); i++ ) {
			
			tokens[i] = tokensList.get(i).tokenText.toString();
			
			tags[i] = posTags.get(i).tagValue.toString();
			
		}
		
		String[] chunks = chunker.chunk(tokens, tags);

		List<NounPhrase> nounPhrases = new ArrayList<NounPhrase>();
		
		List<VerbPhrase> verbPhrases = new ArrayList<VerbPhrase>();
		
		for (int i=0; i<chunks.length; i++) {
		
			if (chunks[i].startsWith("B") && chunks[i].endsWith("NP")) {
				
				int start = i;
				
				while(i+1<chunks.length && chunks[i+1].endsWith("NP")) {
					i++;
				}
				
				int end = i;
				
//				if (i+1 <chunks.length) {
//					end -=1;
//				}
				
				NounPhrase np = new NounPhrase();
				nounPhrases.add(np);
				np.setURI(s.getURI() + "_nounphrase_" + nounPhrases.size());
				np.startTokenIndex = start;
				np.endTokenIndex = end;
				
			}
			
	
			if (chunks[i].startsWith("B") && chunks[i].endsWith("VP")) {
				
				int start =i;
				
				while(i+1<chunks.length && chunks[i+1].endsWith("VP")) {
					i++;
				}
				
				int end = i;
				
//				if (i+1 <chunks.length) {
//					end -=1;
//				}
				
				VerbPhrase vp = new VerbPhrase();
				verbPhrases.add(vp);
				vp.setURI(s.getURI() + "_verbphrase_" + verbPhrases.size());
				vp.startTokenIndex = start;
				vp.endTokenIndex = end;
				
			}
			
		}
	  
		results.addAll(nounPhrases);
		results.addAll(EdgeUtils.createEdges(s, nounPhrases, Edge_hasNounPhrase, VitalOntology.Edge_hasNounPhraseURIBase));
		
		results.addAll(verbPhrases);
		results.addAll(EdgeUtils.createEdges(s, verbPhrases, Edge_hasVerbPhrase, VitalOntology.Edge_hasVerbPhraseURIBase));
		
//		s.setNounPhrases(nounPhrases);
//		s.setVerbPhrases(verbPhrases);
		
	}
	
	
	
	
	
}
