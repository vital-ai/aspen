package ai.vital.aspen.groovy.nlp.m3rd;

import opennlp.tools.tokenize.WhitespaceTokenizer;

import java.util.ArrayList;
import java.util.List;

import ai.vital.domain.Document;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.domain.Token;
import ai.vital.aspen.groovy.nlp.model.TokenUtils;
import edu.cmu.minorthird.text.TextToken;
import edu.cmu.minorthird.text.Tokenizer;
public class DocumentTokenizer_VS implements Tokenizer {

	Document doc;
	
	public DocumentTokenizer_VS(Document doc) {
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
		
		List tbs = doc.getTextBlocks();
		
		for(TextBlock b : tbs) {
			
			String text = b.text;
			
			List sentences = b.getSentences();
			
			for(Sentence s : sentences) {
				
				int sentenceOffset = s.startPosition.rawValue();
				
				List<Token> tokens = TokenUtils.getTokens(s);//s.getTokens();
				
				for(Token t : tokens) {
					
					int tokenAbsoluteOffset = blockOffset + sentenceOffset + t.startPosition.rawValue();
					
					res.add(new TextToken(_d, tokenAbsoluteOffset, t.endPosition.rawValue() - t.startPosition.rawValue()));
					
				}
				
			}
		
			blockOffset += ( text.length() + 1 );
			
		}
		
		return res.toArray(new TextToken[res.size()]);
	}
	
}
