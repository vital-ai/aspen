package ai.vital.aspen.utils.stemmer

import edu.mit.jwi.morph.WordnetStemmer

import edu.mit.jwi.Dictionary

import edu.mit.jwi.item.POS


class VitalWordnetStemmer {

	
	String path = "/Users/hadfield/Local/vital-git/aspen/aspen-groovy/resources/wordnet/WordNet-3.0/dict"
	
	WordnetStemmer stemmer = null
	
	
	public void init() {
		
		Dictionary dict = new Dictionary(new File(path))
		dict.open();
		stemmer = new WordnetStemmer(dict);
		
		
		
		
	}
	
	// pass in POS type?
	// leaving it out tries all cases and returns a match selecting the first that was stemmed?
	
	public String stem(String word) {
		
		
		List<String> test = stemmer.findStems(word, POS.NOUN);
		for (int i = 0; i < test.size(); i++) {
			System.out.println(test.get(i));
		}
		
		
		return test.get(0)
		
	}
	
	
	
	
}
