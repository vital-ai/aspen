package ai.vital.opennlp.tokenizer

import java.util.regex.Matcher
import java.util.regex.Pattern;

import opennlp.tools.tokenize.AbstractTokenizer
import opennlp.tools.tokenize.SimpleTokenizer

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import opennlp.tools.util.Span;
import opennlp.tools.util.StringUtil;

/**
 * A tokenizer that uses simple tokenizer but rejoins decimal numbers, emails and URL addresses 
 * @author Derek
 *
 */
class VitalSimpleTokenizer extends SimpleTokenizer {

	public static VitalSimpleTokenizer INSTANCE = null;
	
	static {
		INSTANCE = new VitalSimpleTokenizer()
	}

//	Pattern decimalPattern = Pattern.compile("(^|\\s)(\\d*\\.?\\d*)(\\s|\$)")

	Pattern emailPattern = Pattern.compile("(^|\\s)([A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,4})(\\s|\$)", Pattern.CASE_INSENSITIVE)

	//	Pattern urlPattern = Pattern.compile("(^|\\s)((https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w\\.-]*)*\\/?)(\\s|\$)")

	Pattern urlPattern = Pattern.compile("(^|\\s)(https?:\\S+)(\\s|\$)")



	public Span[] origTokenizePos(String s) {
		
		CharacterEnum charType = CharacterEnum.WHITESPACE;
		CharacterEnum state = charType;
		List<Span> tokens = new ArrayList<Span>();
		int sl = s.length();
		int start = -1;
		char pc = 0;
		for (int ci = 0; ci < sl; ci++) {
			char c = s.charAt(ci);
			
			boolean isNumberElement = false
			
			if(c == ',' || c == '.') {
				
				//must be in the middle of digits?
				if(ci > 0 && ci < sl -1 ) {
					if( Character.isDigit(s.charAt(ci-1)) && Character.isDigit(s.charAt(ci+1)) ) {
						isNumberElement = true
					}
				}
				
			}
			
			
			if (StringUtil.isWhitespace(c)) {
				charType = CharacterEnum.WHITESPACE;
			}
			else if (Character.isLetter(c)) {
				charType = CharacterEnum.ALPHABETIC;
			}
			else if (Character.isDigit(c) || isNumberElement) {
				charType = CharacterEnum.NUMERIC;
			}
			else {
				charType = CharacterEnum.OTHER;
			}
			if (state == CharacterEnum.WHITESPACE) {
				if (charType != CharacterEnum.WHITESPACE) {
					start = ci;
				}
			}
			else {
				if (charType != state || charType == CharacterEnum.OTHER && c != pc) {
					tokens.add(new Span(start, ci));
					start = ci;
				}
			}
			state = charType;
			pc = c;
		}
		if (charType != CharacterEnum.WHITESPACE) {
			tokens.add(new Span(start, sl));
		}
		return tokens.toArray(new Span[tokens.size()]);
	}


	@Override
	public Span[] tokenizePos(String s) {

		List<Span> vitalSpans = []

		/*
		Matcher decimalMatcher = decimalPattern.matcher(s);
		while(decimalMatcher.find()) {
			int st = decimalMatcher.start(2)
			int end = decimalMatcher.end(2)
			vitalSpans.add(new Span(st, end, "decimal_number"));
		}
		*/
		
		Matcher emailMatcher = emailPattern.matcher(s)
		while(emailMatcher.find()) {
			int st = emailMatcher.start(2)
			int end = emailMatcher.end(2)
			vitalSpans.add(new Span(st, end, "email"));
		}

		Matcher urlMatcher = urlPattern.matcher(s)
		while(urlMatcher.find()) {
			int st = urlMatcher.start(2)
			int end = urlMatcher.end(2)
			vitalSpans.add(new Span(st, end, "url"));
		}

		List<Span> original = new ArrayList<Span>(Arrays.asList(this.origTokenizePos(s)));

		for( Iterator<Span> iter = original.iterator(); iter.hasNext(); ) {

			Span sp = iter.next();

			for(Span vitalSpan : vitalSpans) {

				if( vitalSpan.intersects(sp) ) {
					iter.remove()
					break
				}

			}

		}

		original.addAll(vitalSpans)

		Collections.sort(original)

		return original



		/* 2nd attempt to rejoin spans and check string - less efficient
		 Span[] original = super.tokenizePos(s);
		 List<Span> output = []
		 List<Span> group = []
		 for(int i = 0 ; i < original.length; i++) {
		 Span c = original[i]
		 if(group.size() > 0) {
		 //check if there's something to re-join
		 Span prev = group.last()
		 if(prev.end == c.start) {
		 //fall through					
		 } else {
		 List<Span> x = handleGroup(s, group);
		 output.addAll(x)
		 //release the group
		 group.clear()
		 }
		 }
		 group.add(c) 	
		 }
		 List<Span> x = handleGroup(s, group);
		 output.addAll(x)
		 return output
		 */

	}

	/*
	 protected List<Span> handleGroup(String s, List<Span> group) {
	 if(group.size() < 2) return group
	 String str = s.substring( group.first().start, group.)
	 }
	 */


}

class CharacterEnum {
	static final CharacterEnum WHITESPACE = new CharacterEnum("whitespace");
	static final CharacterEnum ALPHABETIC = new CharacterEnum("alphabetic");
	static final CharacterEnum NUMERIC = new CharacterEnum("numeric");
	static final CharacterEnum OTHER = new CharacterEnum("other");
	private String name;
	private CharacterEnum(String name) {
		this.name = name;
	}
	@Override
	public String toString() {
		return name;
	}
}
