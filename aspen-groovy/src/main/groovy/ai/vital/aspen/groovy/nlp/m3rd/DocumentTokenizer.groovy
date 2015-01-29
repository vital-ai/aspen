/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.m3rd;

import java.util.ArrayList;
import java.util.List;

import opennlp.tools.tokenize.WhitespaceTokenizer;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Token;
import edu.cmu.minorthird.text.TextToken;
import edu.cmu.minorthird.text.Tokenizer;

public class DocumentTokenizer implements Tokenizer {

	Document doc;
	
	public DocumentTokenizer(Document doc) {
		super();
		this.doc = doc;
	}

	@Override
	public String[] splitIntoTokens(String input) {

		return WhitespaceTokenizer.INSTANCE.tokenize(input);
		
		/*
		if(input.startsWith("\"") && ( input.endsWith("\"") || input.endsWith("\" ") ) && input.contains("txt")) {
			//m3rd trie definition!
			return WhitespaceTokenizer.INSTANCE.tokenize(input);
		}
		*/
		
		//ignore the input, use the source doc for tokens
		/*
		List<String> res = new ArrayList<String>();
		
		for( TextBlock b : doc.getTextBlocks() ) {
			
			for( Sentence s : b.getSentences() ) {
				
				List<Token> tokens = s.getTokens();
				
				for(Token t : tokens) {
					
					res.add(t.getText());
					
				}
				
			}
			
		}
		
		return res.toArray(new String[res.size()]);
		*/
	}

	@Override
	public TextToken[] splitIntoTokens(edu.cmu.minorthird.text.Document _d) {

		//ignore the source do, use source document 
		
		List<TextToken> res = new ArrayList<TextToken>();
		
		int blockOffset = 0;
		
		for(TextBlock b : doc.getTextBlocks()) {
			
			String text = b.getText();
			
			for(Sentence s : b.getSentences()) {
				
				int sentenceOffset = s.getStart();
				
				List<Token> tokens = s.getTokens();
				
				for(Token t : tokens) {
					
					int tokenAbsoluteOffset = blockOffset + sentenceOffset + t.getStart();
					
					res.add(new TextToken(_d, tokenAbsoluteOffset, t.getEnd() - t.getStart()));
					
				}
				
			}
		
			blockOffset += ( text.length() + 1 );
			
		}
		
		return res.toArray(new TextToken[res.size()]);
	}
	
}
