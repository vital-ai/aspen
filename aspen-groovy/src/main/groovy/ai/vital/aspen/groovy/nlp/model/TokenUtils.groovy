package ai.vital.aspen.groovy.nlp.model

import com.vitalai.domain.nlp.Sentence
import com.vitalai.domain.nlp.Token

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
