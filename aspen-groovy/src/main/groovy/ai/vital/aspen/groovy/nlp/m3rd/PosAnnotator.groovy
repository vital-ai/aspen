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

import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.PosTag;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Token;
import edu.cmu.minorthird.text.CharAnnotation;
import edu.cmu.minorthird.text.Span;
import edu.cmu.minorthird.text.StringAnnotator;
import edu.cmu.minorthird.text.TextLabels;

public class PosAnnotator extends StringAnnotator {

	private Document doc;
	
	public PosAnnotator(Document doc) {
		super();
		this.doc = doc;
		providedAnnotation = "pos";
	}

	@Override
	public String explainAnnotation(TextLabels labels, Span span) {
		return "DUNNO!";
	}

	@Override
	protected CharAnnotation[] annotateString(String input) {

	    //list of annotations
	    List<CharAnnotation> list = new ArrayList<CharAnnotation>();
		
		int blockOffset = 0;
		
		for(TextBlock b : doc.getTextBlocks()) {
			
			for( Sentence s : b.getSentences() ) {
				
				List<PosTag> posTags = s.getPosTags();
				
				List<Token> tokens = s.getTokens();
				
				for(int i = 0 ; i < posTags.size(); i++) {
					
					PosTag posTag = posTags.get(i);
					Token token = tokens.get(i);
					
					String tag = posTag.getTag();
					
					int tokenOffset = blockOffset + s.getStart() + token.getStart();
					
					//put into list
					CharAnnotation ca = new CharAnnotation(tokenOffset, token.getEnd() - token.getStart(), tag);
					
					list.add(ca);
					
				}
				
			}
			
			blockOffset = blockOffset + b.getText().length() + 1;
			
		}
		
		return list.toArray(new CharAnnotation[list.size()]);
		
	}

}
