/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.model

import ai.vital.domain.Sentence
import ai.vital.domain.Token

class TokenUtils {

	public static List<Token> getTokens(Sentence sentence) {
		
		List<Token> tokens = new ArrayList<Token>();
		
		String[] posPairs = sentence.tokensPositionsString.split(' ');
		String[] values = sentence.tokensTextString.split(' ');
		
		if(posPairs.length != values.length) throw new RuntimeException("Incorrect arrays size!");
		
		for(int i = 0 ; i < values.length; i++) {
			
			Token token = new Token();
			
			String[] positions = posPairs[i].split(':');
			
			token.startPosition = Integer.parseInt(positions[0]);
			token.endPosition = Integer.parseInt(positions[1]);
			token.tokenText = values[i];
			token.setURI("" + tokens.size());
			tokens.add(token);

						
		}
		
		return tokens;
		
	}
	
}
