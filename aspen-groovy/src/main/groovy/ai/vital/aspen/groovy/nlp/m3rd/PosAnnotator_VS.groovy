package ai.vital.aspen.groovy.nlp.m3rd;

import java.util.ArrayList;
import java.util.List;

import ai.vital.domain.Document;
import ai.vital.domain.PosTag;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.domain.Token;
import ai.vital.aspen.groovy.nlp.model.PosTagsUtils;
import ai.vital.aspen.groovy.nlp.model.TokenUtils;
import edu.cmu.minorthird.text.AbstractAnnotator;
import edu.cmu.minorthird.text.CharAnnotation;
import edu.cmu.minorthird.text.MonotonicTextLabels;
import edu.cmu.minorthird.text.Span;
import edu.cmu.minorthird.text.StringAnnotator;
import edu.cmu.minorthird.text.TextLabels;

public class PosAnnotator_VS extends StringAnnotator {

	private Document doc;
	
	public PosAnnotator_VS(Document doc) {
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
				
				List<PosTag> posTags = PosTagsUtils.getPosTags(s);//s.getPosTags();
				
				List<Token> tokens = TokenUtils.getTokens(s);//s.getTokens();
				
				for(int i = 0 ; i < posTags.size(); i++) {
					
					PosTag posTag = posTags.get(i);
					Token token = tokens.get(i);
					
					String tag = posTag.tagValue
					
					int tokenOffset = blockOffset + s.startPosition.rawValue() + token.startPosition.rawValue();
					
					//put into list
					CharAnnotation ca = new CharAnnotation(tokenOffset, token.endPosition.rawValue() - token.startPosition.rawValue(), tag);
					
					list.add(ca);
					
				}
				
			}
			
			blockOffset = blockOffset + b.text.length() + 1;
			
		}
		
		return list.toArray(new CharAnnotation[list.size()]);
		
	}

}
