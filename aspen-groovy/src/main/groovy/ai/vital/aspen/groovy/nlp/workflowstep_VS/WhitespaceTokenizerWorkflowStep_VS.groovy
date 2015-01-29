/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep_VS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.*;

import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.Span;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasToken;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.flow.server.ontology.VitalOntology;
import ai.vital.vitalsigns.global.GlobalHashTable;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepV2Impl;

public class WhitespaceTokenizerWorkflowStep_VS extends
		WorkflowStepV2Impl<NLPServerConfig> {

	public final static StepName WHITESPACETOKENIZER_VS = new StepName("whitespacetokenizer_vs");
	
	private final static Logger log = LoggerFactory.getLogger(WhitespaceTokenizerWorkflowStep_VS.class);

	private WhitespaceTokenizer whitespaceTokenizer;
	
	private Pattern lastSentTokenPattern = Pattern.compile("([^,?!.]+)([,?!.]+)", Pattern.CASE_INSENSITIVE);
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);
		log.info("Initializing whitespace tokenizer...");
		whitespaceTokenizer = WhitespaceTokenizer.INSTANCE;
	}
	
	@Override
	public String getName() {
		return WHITESPACETOKENIZER_VS.getName();
	}

	@Override
	public void processPayload(Payload payload)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		for( Document doc : payload.iterator(Document.class) ) {
		
			String docUri = doc.getURI();
			
			log.info("Processing document {} ...", docUri);
			
			List tbs = doc.getTextBlocks();
			
			log.info("There's textBlocks: " + tbs.size());
			
			int tbindex = 1;
			
			for(TextBlock b : tbs) {
				
				long start = System.currentTimeMillis(); 
				List sentences = b.getSentences();
				long stop = System.currentTimeMillis(); 
				
				log.info("Processing tb: " + tbindex++ + " of " + tbs.size() + " with " + sentences.size() + " sentences, " + (stop-start) + "ms - HASH SIZE: " + GlobalHashTable.get().size());
								
				for(Sentence sentence : sentences) {
					
					List<Token> tokens = new ArrayList<Token>();
					
					String t = b.text.substring(sentence.startPosition, sentence.endPosition);
					
					Span[] tokenizePos = whitespaceTokenizer.tokenizePos(t);
				
					StringBuilder tokensText = new StringBuilder();
					
					StringBuilder tokensPositions = new StringBuilder();
					
					for(int i = 0 ; i < tokenizePos.length; i++) {
						
						Span span = tokenizePos[i];
						
						int startPosition = span.getStart();
						int endPosition = span.getEnd();

						String tokenText = span.getCoveredText(t).toString();
						
						String sentenceEnd = null;
						
						String extraToken = null;
						
						//special case to get rid of sentence end punctuation being part of last token 
						if( i == tokenizePos.length - 1) {
							
							Matcher m = lastSentTokenPattern.matcher(tokenText);
							
							if(m.matches()) {
								
								tokenText = m.group(1);

								sentenceEnd = m.group(2);
								
								//back a little
								endPosition = endPosition - sentenceEnd.length();
								
																
							}
							
						} else {
						
							if(tokenText.length() > 2 && tokenText.endsWith(",")) {
								
								tokenText = tokenText.substring(0, tokenText.length()-1);
								
								endPosition--;
							
								extraToken = ",";
									
							}
							
						}
						
						
						
						if(tokensText.length() > 0) {
							tokensText.append(' ');
						}
						
						tokensText.append(tokenText);
						
						if(tokensPositions.length() >0) {
							tokensPositions.append(' ');
						}
						
						tokensPositions.append(startPosition + ":" + endPosition);
						
						if(sentenceEnd != null) {
							tokensText.append(' ').append(sentenceEnd);
							tokensPositions.append(' ').append(endPosition + ":" + (endPosition + sentenceEnd.length()));
						}
						
						if(extraToken != null) {
							tokensText.append(' ').append(extraToken);
							tokensPositions.append(' ').append(endPosition + ":" + (endPosition + extraToken.length()));
							
						}
						
//						Token token = new Token();
						/*
						token.startPosition = span.getStart();
						token.endPosition = span.getEnd();
						token.tokenText = span.getCoveredText(t).toString();
						tokens.add(token);
						token.setURI(sentence.getURI() + "_token_" + tokens.size());
						*/
					}
					
					sentence.tokensPositionsString = tokensPositions.toString();
					sentence.tokensTextString = tokensText.toString();
					
//					sentence.setTokens(tokens);
					/*
					payload.putGraphObjects(tokens);
					payload.putGraphObjects(EdgeUtils.createEdges(sentence, tokens, Edge_hasToken, VitalOntology.Edge_hasTokenURIBase));
					*/
				}
				
			}
			
		}
		
	}

}