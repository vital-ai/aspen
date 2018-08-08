package ai.vital.aspen.utils.analyzer

import ai.vital.aspen.utils.stemmer.impl.SnowballStemmer
import ai.vital.aspen.utils.stemmer.impl.ext.EnglishStemmer

class VitalSimpleAnalyzer {

	SnowballStemmer stemmer = null
	
	
	public void init() {
		
		stemmer = new EnglishStemmer()
		
	}
	
	public String stem(String text) {
	
		stemmer.setCurrent(text);

		stemmer.stem();
	
		String output = stemmer.getCurrent()
	
		stemmer.setCurrent("")
	
	
	return output
	
	}
	
	
}
