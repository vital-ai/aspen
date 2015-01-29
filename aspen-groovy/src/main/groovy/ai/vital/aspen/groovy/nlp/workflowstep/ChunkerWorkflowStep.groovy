/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import opennlp.tools.chunker.ChunkerME;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.config.OpenNLPConfig;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.NounPhrase;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.VerbPhrase;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.aspen.groovy.nlp.models.ChunkerModelWrapper;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepImpl;

import com.hp.hpl.jena.rdf.model.Model;

public class ChunkerWorkflowStep extends WorkflowStepImpl<NLPServerConfig> {

	public final static StepName CHUNKER = new StepName("chunker");
	
	private final static Logger log = LoggerFactory.getLogger(ChunkerWorkflowStep.class);
	
	private ChunkerME chunker;
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);
		
		OpenNLPConfig openNLP = config.getOpenNLP();
		File modelFile = new File(openNLP.getModelsDir(), openNLP.getChunkerModel());
		
		log.info("Initializing Chunkder model from file: {}", modelFile.getAbsolutePath());
		
		long start = System.currentTimeMillis();
		
		ChunkerModelWrapper.init(modelFile);
		
		chunker = ChunkerModelWrapper.getChunker();
		
		long stop = System.currentTimeMillis();
		
		log.info("Chunker model obtained, {}ms", stop - start);
		
		super.init(config);
		
	}
	
	@Override
	public String getName() {
		return CHUNKER.getName();
	}

	@Override
	public Model processModel(Model model, Map<String, Serializable> context)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {
		
		List<Document> docs = DocumentExtractor.readDocuments(model);
		
		for( Document doc : docs ) {
			
			String docUri = doc.getUri();
			
			log.debug("Processing document {} ...", docUri);
			
			for( TextBlock b : doc.getTextBlocks() ) {
				

				for(Sentence s : b.getSentences()) {
					
					processSentence(docUri, s);
					
				}
				
				
			}
		
			DocumentExtractor.updateDoc(model, doc);
			
		}
		
		return model;
	}

	public void processSentence(String docUri, Sentence s) {
		
		List<Token> tokensList = s.getTokens();
		
		String[] tags = new String[tokensList.size()];
		
		String[] tokens = new String[tokensList.size()];
		
		for( int i = 0 ; i < tokensList.size(); i++ ) {
			
			tokens[i] = tokensList.get(i).getText();
			
			tags[i] = s.getPosTags().get(i).getTag();
			
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
				np.setUri(s.getUri() + "_nounphrase_" + nounPhrases.size());
				np.setStartTokenIndex(start);
				np.setEndTokenIndex(end);
				
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
				vp.setUri(s.getUri() + "_verbphrase_" + verbPhrases.size());
				vp.setStartTokenIndex(start);
				vp.setEndTokenIndex(end);
				
			}
			
		}
	  
		s.setNounPhrases(nounPhrases);
		s.setVerbPhrases(verbPhrases);
	}
}
