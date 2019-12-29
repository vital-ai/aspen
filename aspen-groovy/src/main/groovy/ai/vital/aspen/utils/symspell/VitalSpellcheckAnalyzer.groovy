package ai.vital.aspen.utils.symspell

import io.github.mightguy.spellcheck.symspell.api.DataHolder
import io.github.mightguy.spellcheck.symspell.exception.SpellCheckException
import io.github.mightguy.spellcheck.symspell.impl.SymSpellCheck
import io.github.mightguy.spellcheck.symspell.api.StringDistance
import io.github.mightguy.spellcheck.symspell.common.Composition;
import io.github.mightguy.spellcheck.symspell.common.DictionaryItem;
import io.github.mightguy.spellcheck.symspell.common.Murmur3HashFunction;
import io.github.mightguy.spellcheck.symspell.common.QwertyDistance
import io.github.mightguy.spellcheck.symspell.common.SpellCheckSettings;
import io.github.mightguy.spellcheck.symspell.common.SuggestionItem;
import io.github.mightguy.spellcheck.symspell.common.Verbosity
import io.github.mightguy.spellcheck.symspell.common.WeightedDamerauLevenshteinDistance;
import io.github.mightguy.spellcheck.symspell.impl.InMemoryDataHolder;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.jar.JarInputStream

class VitalSpellcheckAnalyzer {

	private DataHolder dataHolder;
	private SymSpellCheck symSpellCheck;
  
  
	private boolean init() throws IOException, SpellCheckException {
		
		
	 // ClassLoader classLoader = VitalSpellcheckAnalyzer.class.getClassLoader();
	  
	  SpellCheckSettings spellCheckSettings = SpellCheckSettings.builder()
	  .countThreshold(1)
	  .deletionWeight(0.7)
	  .insertionWeight(0.7)
	  .replaceWeight(1)
	  .maxEditDistance(2)
	  .transpositionWeight(1)
	  .topK(5)
	  .prefixLength(10)
	  .verbosity(Verbosity.ALL).build();
	  
		StringDistance weightedDamerauLevenshteinDistance =
	  new WeightedDamerauLevenshteinDistance(
		  spellCheckSettings.getDeletionWeight(),
		  spellCheckSettings.getInsertionWeight(),
		  spellCheckSettings.getReplaceWeight(),
		  spellCheckSettings.getTranspositionWeight(),
		  new QwertyDistance()); // new QwertyDistance() // null
	  
	  
	  /*
	  WeightedDamerauLevenshteinDistance weightedDamerauLevenshteinDistance =
		  new WeightedDamerauLevenshteinDistance(spellCheckSettings.getDeletionWeight(),
			  spellCheckSettings.getInsertionWeight(), spellCheckSettings.getReplaceWeight(),
			  spellCheckSettings.getTranspositionWeight(), null);
		  
	  */
		  
		  
	  dataHolder = new InMemoryDataHolder(spellCheckSettings, new Murmur3HashFunction());
  
	  // paths to be adjusted to be same as model loading in aspen?
	  
	  // unigrams and bigrams to include values from the datasets, such as business names
	  
	  
	  symSpellCheck = new SymSpellCheck(dataHolder, weightedDamerauLevenshteinDistance,
		  spellCheckSettings);
	  
	  println "Loading unigram from source: " + "frequency_dictionary_en_82_765.txt"
	  
	  loadUniGramFile(
		  VitalSpellcheckAnalyzer.getResourceAsStream("/frequency_dictionary_en_82_765.txt")
		  );
	  
		println "Loading bigram from resource: " + "frequency_bigramdictionary_en_243_342.txt" 
		  
	  loadBiGramFile(
		  VitalSpellcheckAnalyzer.getResourceAsStream("/frequency_bigramdictionary_en_243_342.txt")
		  );
  
	  return false;
	}
  
	private void loadUniGramFile(InputStream file) throws IOException, SpellCheckException {
		
	  BufferedReader br = new BufferedReader(new InputStreamReader(file, "UTF-8"))
	  
		String line;
		while ((line = br.readLine()) != null) {
			
			//println "Line: " + line
			
		  String[] arr = line.split("\\s+");
		  dataHolder.addItem(new DictionaryItem(arr[0], Double.parseDouble(arr[1]), -1.0));
		}
	  
		//println "unigram dataHolder size: " + dataHolder.size
		
		
	}
  
	private void loadBiGramFile(InputStream file) throws IOException, SpellCheckException {
		
	  BufferedReader br = new BufferedReader(new InputStreamReader(file, "UTF-8"))
	  
		String line;
		while ((line = br.readLine()) != null) {
			
			//println "Line: " + line
			
		  String[] arr = line.split("\\s+");
		  dataHolder
			  .addItem(new DictionaryItem(arr[0] + " " + arr[1], Double.parseDouble(arr[2]), -1.0));
		}
		
		
		//println "bigram dataHolder size: " + dataHolder.size
	  
		
		
	}
  
	private void suggestItem(String[] args) throws IOException, SpellCheckException {
	  if (args.length > 0) {
		suggestItemOnArgs(args[0]);
	  } else {
		suggestItemOnConsole();
	  }
  
	}
  
	private void suggestItemOnArgs(String inputTerm) throws IOException, SpellCheckException {
		
	   // use for single terms
		 
	  //List<SuggestionItem> suggestions = symSpellCheck.lookup(inputTerm);
	  
	  //List<SuggestionItem> suggestions = symSpellCheck.lookup("pollicy");
	  
	  //println "Suggestions: " + suggestions
	  
	  
	  // get multiple compound suggestions
	  //List<SuggestionItem> suggestions = symSpellCheck.lookupCompound(inputTerm);
	  
	  
	  //List<SuggestionItem> suggestions = symSpellCheck.lookupCompound(inputTerm)
	  
		// use for single word suggestions
	  List<SuggestionItem> suggestions = symSpellCheck.lookup("abacs", Verbosity.ALL, 2.0)
	  
	  
	  println "Suggestions: " + suggestions
	  
	  // how to get more than one compound suggestion?
	  // i always get exactly one
	  // implementation seems to only select one, so perhaps just not implemented yet
	  
	  
	  
	  suggestions //.stream()
		  //.limit(10)
		  .each { suggestion -> System.out.println(
			  "Lookup suggestion: "
				  + suggestion.getTerm() + " "
				  + suggestion.getDistance() + " "
				  + suggestion.getCount())
		  }
	  
	  SuggestionItem compound = symSpellCheck.lookupCompound(inputTerm).get(0);
		  
				  
	  Composition composition = symSpellCheck.wordBreakSegmentation(inputTerm, 10, 2);
		  
	  System.out.println("LookupCompound: " + compound.getTerm());
	  
	  System.out.println("Composition: " + composition.getCorrectedString());
	  
	}
  
	private void suggestItemOnConsole() throws IOException, SpellCheckException {
	  String inputTerm;
	  BufferedReader reader =
		  new BufferedReader(new InputStreamReader(System.in));
	  while (true) {
		System.out.println("Please enter the term to get the suggest Item");
		inputTerm = reader.readLine();
		if (inputTerm.equalsIgnoreCase("q")) {
		  return;
		}
		suggestItemOnArgs(inputTerm);
	  }
	}
  
	public static void main(String[] args) throws IOException, SpellCheckException {
		
	  // migrate library into aspen?
		  
		
		
	  //String[] phrase = ["insurrance pollicy"]
		
		
	  String[] phrase = ["abacs brokers"]
	  
	  
	  VitalSpellcheckAnalyzer  spellCheckerConsole = new VitalSpellcheckAnalyzer();
	  
	  
	  //ClassLoader classLoader = SymSpellCheck.class.getClassLoader();
	  
			  
	  //File f = new File( TestQueryCorrection.getResource("/frequency_dictionary_en_82_765.txt").toURI())
	  
	  //println f
	  
			  
	  //spellCheckerConsole.loadUniGramFile(f)
	  
	  
	  spellCheckerConsole.init();
	  
	  println "Input: " + phrase[0]
	  
	  spellCheckerConsole.suggestItem(phrase);
	}
	
	
}
