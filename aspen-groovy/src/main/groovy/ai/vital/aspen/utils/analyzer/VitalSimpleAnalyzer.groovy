package ai.vital.aspen.utils.analyzer

import ai.vital.aspen.utils.stemmer.impl.SnowballStemmer
import ai.vital.aspen.utils.stemmer.impl.ext.EnglishStemmer
import opennlp.tools.tokenize.TokenizerME
import opennlp.tools.tokenize.TokenizerModel

class VitalSimpleAnalyzer {

	
	// get resources out of jar
	
	
	// Simple Text Analyzer for use cases like document indexing and text queries
	
	// Handle tokenization, stop words, and stemming
	// tokenization to handle splitting punctuation
	// lowercase text
	
	
	// decide if punctuation should be included, removed, or treated as stopword
	
	
	
	// depending on stemmer selection the analysis might not produce dictionary words,
	// which means the results may not be usable for looking up word vectors
	// example fantastic --> fantast
	
	// some word vectors may include these fragments, whereas others might be more normalized
	
	// a fix would be configuring analysis with WordnetStemmer instead of SnowballStemmer
	
	SnowballStemmer stemmer = null
	
	
	TokenizerME tokenizer = null
	
	
	public VitalSimpleAnalyzer() {
		
		stemmer = new EnglishStemmer()
		
		InputStream inputStream = new FileInputStream("/Users/hadfield/Local/vital-git/aspen/aspen-groovy/resources/models/en-token.bin");
		
		
		TokenizerModel tokenModel = new TokenizerModel(inputStream);
		
		
		tokenizer = new TokenizerME(tokenModel);
		
		
	}
	
	
	public String analyze(String input) {
		
		
		// lowercase and tokenize
		String[] tokens = tokenize(input.toLowerCase())
		
		
		def token_removedstopwords = []
		
		// replace stop words
		for(t in tokens) {
			
			if(t in this.default_stopwords) { token_removedstopwords.add(this.default_stopword_token)  }
			else { token_removedstopwords.add(t) }
			
		}
		
		def tokens_stemmed = []
		
		
		
		// stem all words
		for(t in token_removedstopwords) {
			
			
			def s = stem(t)
			
			tokens_stemmed.add(s)
			
		}
		
	
		//join into a string, which will be separated by white space
		
		String analyzed_text = tokens_stemmed.join(" ")
		
		
		
		
		return analyzed_text
	}
	
	
	
	
	
	public String[] tokenize(String text) {
		
		
		
		String[] tokens = tokenizer.tokenize(text);
		
		
		return tokens
		
	}
	
	
	public String stem(String word) {
	
		if(word == this.default_stopword_token) { return word }
		
		
		
		stemmer.setCurrent(word);

		stemmer.stem();
	
		String output = stemmer.getCurrent()
	
		stemmer.setCurrent("")
	
	
	return output
	
	}
	
	
	String default_stopword_token = "_STOP_"
	
	// default english stop words
	
	// check on tokenization to match up contractions
	// wouldn't tokenizes to: would, n't, so "n't" perhaps should be a stop word
	
	
	
	String[] default_stopwords =
	["n't", 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "'re", "'ve",
		 "'ll", "'d", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 
		 'she', "'s", 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 
		 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 
		 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 
		 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 
		 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 
		 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 
		 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 
		 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 'can', 
		 'will', 'just', 'should', 'now', 'ain', 
		 'could',  
		 'might', 'must', 'need', 'sha', 
		 'should', 'was', 'were', 'would']
	
	
}
