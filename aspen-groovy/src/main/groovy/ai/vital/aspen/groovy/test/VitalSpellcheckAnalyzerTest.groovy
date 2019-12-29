package ai.vital.aspen.groovy.test

import ai.vital.aspen.utils.symspell.VitalSpellcheckAnalyzer
import io.github.mightguy.spellcheck.symspell.common.Composition
import io.github.mightguy.spellcheck.symspell.common.SuggestionItem

import io.github.mightguy.spellcheck.symspell.common.Verbosity


class VitalSpellcheckAnalyzerTest {

	static main(args) {
		
		//String[] phrase = ["insurrance pollicy"]
		
	String[] phrase = ["abacs brokers"]
	
	VitalSpellcheckAnalyzer spellChecker = new VitalSpellcheckAnalyzer();
	
	spellChecker.init();
	
	println "Input: " + phrase[0]
	
	spellChecker.suggestItem(phrase);
	
	
	
	List<SuggestionItem> suggestions = spellChecker.symSpellCheck.lookup("abacs", Verbosity.ALL, 2.0)
	
	
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
	
	SuggestionItem compound = spellChecker.symSpellCheck.lookupCompound(phrase[0]).get(0);
		
				
	Composition composition = spellChecker.symSpellCheck.wordBreakSegmentation(phrase[0], 10, 2);
		
	System.out.println("LookupCompound: " + compound.getTerm());
	
	System.out.println("Composition: " + composition.getCorrectedString());
	
	
		
		
	}

}
