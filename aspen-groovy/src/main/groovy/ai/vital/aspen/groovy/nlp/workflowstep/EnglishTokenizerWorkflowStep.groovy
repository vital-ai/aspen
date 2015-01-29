/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.config.OpenNLPConfig;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.IWorkflowStep.ProcessflowHaltException
import ai.vital.workflow.IWorkflowStep.WorkflowHaltException
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepImpl;

public class EnglishTokenizerWorkflowStep extends WorkflowStepImpl<NLPServerConfig> {

	public final static StepName ENGLISHTOKENIZER = new StepName("englishtokenizer");
	
	private final static Logger log = LoggerFactory.getLogger(EnglishTokenizerWorkflowStep.class);
	
	private TokenizerME tokenizerME;
	
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);
		OpenNLPConfig openNLP = config.getOpenNLP();
		File modelFile = new File(openNLP.getModelsDir(), openNLP.getEnglishTokenizerModel());
		
		log.info("Initializing english tokenizer model from file: {}", modelFile.getAbsolutePath());
		
		long start = System.currentTimeMillis();
		
		try {
			tokenizerME = new TokenizerME(new TokenizerModel(new FileInputStream(modelFile)));
		} catch (IOException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new StepInitializationException(e);
		}
		
		long stop = System.currentTimeMillis();
		
		log.info("English tokenizer model loaded, {}ms", stop - start);
		
	}
	
	@Override
	public String getName() {
		return ENGLISHTOKENIZER.getName();
	}

	@Override
	public Model processModel(Model model, Map<String, Serializable> context)
			throws WorkflowHaltException, ProcessflowHaltException, Exception {
		
		List<Document> docs = DocumentExtractor.readDocuments(model);
		
		for( Document doc : docs ) {
			
			String docUri = doc.getUri();
			
			log.debug("Processing document {} ...", docUri);
			
			for(TextBlock b : doc.getTextBlocks()) {
				
				for(Sentence sentence : b.getSentences()) {
					
					List<Token> tokens = new ArrayList<Token>();
					
					String t = b.getText().substring(sentence.getStart(), sentence.getEnd());
					
					Span[] tokenizePos = tokenizerME.tokenizePos(t);
				
					for(Span span : tokenizePos ) {
						
						Token token = new Token();
						token.setStart(span.getStart());
						token.setEnd(span.getEnd());
						token.setText(span.getCoveredText(t).toString());
						tokens.add(token);
						token.setUri(sentence.getUri() + "_token_" + tokens.size());
						
					}
					
					sentence.setTokens(tokens);
					
				}
				
			}
			
			DocumentExtractor.updateDoc(model, doc);
			
		}
		
		return model;
	}

}
