/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.Span;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepImpl;

public class WhitespaceTokenizerWorkflowStep extends
		WorkflowStepImpl<NLPServerConfig> {

	public final static StepName WHITESPACETOKENIZER = new StepName("whitespacetokenizer");
	
	private final static Logger log = LoggerFactory.getLogger(WhitespaceTokenizerWorkflowStep.class);

	private WhitespaceTokenizer whitespaceTokenizer;
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);
		log.info("Initializing whitespace tokenizer...");
		whitespaceTokenizer = WhitespaceTokenizer.INSTANCE;
	}
	
	@Override
	public String getName() {
		return WHITESPACETOKENIZER.getName();
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
			
			for(TextBlock b : doc.getTextBlocks()) {
				
				for(Sentence sentence : b.getSentences()) {
					
					List<Token> tokens = new ArrayList<Token>();
					
					String t = b.getText().substring(sentence.getStart(), sentence.getEnd());
					
					Span[] tokenizePos = whitespaceTokenizer.tokenizePos(t);
				
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
