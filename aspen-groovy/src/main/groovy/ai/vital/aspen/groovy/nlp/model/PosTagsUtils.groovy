package ai.vital.aspen.groovy.nlp.model

import ai.vital.domain.PosTag;
import ai.vital.domain.Sentence

class PosTagsUtils {

	public static List<PosTag> getPosTags(Sentence sentence) {

		List<PosTag> tags = new ArrayList<PosTag>();
		
		//most probably
		if(!sentence.posTagsValuesString || !sentence.posTagsConfidenceString) return tags;
		
		String[] values = sentence.posTagsValuesString.split(' ');
		String[] confidences = sentence.posTagsConfidenceString.split(' ');

		if(confidences.length != values.length) throw new RuntimeException("Incorrect arrays size!");
				
		for(int i = 0 ; i < values.length; i++) {
			
			PosTag pt = new PosTag();
			pt.confidence = Double.parseDouble(confidences[i]);
			pt.tagValue = values[i];
			pt.setURI("" + tags.size());
			tags.add(pt);
			
		}

		return tags;		
		
	}
	
}
