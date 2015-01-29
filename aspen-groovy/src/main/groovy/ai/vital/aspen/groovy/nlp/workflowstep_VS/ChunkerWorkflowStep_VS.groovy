/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep_VS;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import opennlp.tools.chunker.ChunkerME;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasNounPhrase;
import ai.vital.domain.Edge_hasVerbPhrase;
import ai.vital.domain.NounPhrase;
import ai.vital.domain.PosTag;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.domain.Token;
import ai.vital.domain.VerbPhrase;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.config.OpenNLPConfig;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.model.PosTagsUtils;
import ai.vital.aspen.groovy.nlp.model.TokenUtils;
import ai.vital.aspen.groovy.nlp.models.ChunkerModelWrapper;
import ai.vital.flow.server.ontology.VitalOntology;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepV2Impl;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.vitalsigns.global.GlobalHashTable;

public class ChunkerWorkflowStep_VS extends WorkflowStepV2Impl<NLPServerConfig> {

	public final static StepName CHUNKER_VS = new StepName("chunker_vs");
	
	private final static Logger log = LoggerFactory.getLogger(ChunkerWorkflowStep_VS.class);
	
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
		return CHUNKER_VS.getName();
	}

	@Override
	public void processPayload(Payload payload)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		for( Document doc : payload.iterator(Document.class) ) {
			
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
					
					processSentence(payload, docUri, s);
					
				}
				
			}
		
		}
		
	}

	public void processSentence(Payload payload, String docUri, Sentence s) {
		
		List<Token> tokensList = TokenUtils.getTokens(s);// s.getTokens();
		
		List<PosTag> posTags = PosTagsUtils.getPosTags(s);// s.getPosTags();
		
		String[] tags = new String[tokensList.size()];
		
		String[] tokens = new String[tokensList.size()];
		
		for( int i = 0 ; i < tokensList.size(); i++ ) {
			
			tokens[i] = tokensList.get(i).tokenText;
			
			tags[i] = posTags.get(i).tagValue;
			
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
	  
		payload.putGraphObjects(nounPhrases);
		payload.putGraphObjects(EdgeUtils.createEdges(s, nounPhrases, Edge_hasNounPhrase, VitalOntology.Edge_hasNounPhraseURIBase));
		
		payload.putGraphObjects(verbPhrases);
		payload.putGraphObjects(EdgeUtils.createEdges(s, verbPhrases, Edge_hasVerbPhrase, VitalOntology.Edge_hasVerbPhraseURIBase));
		
//		s.setNounPhrases(nounPhrases);
//		s.setVerbPhrases(verbPhrases);
		
	}
}
