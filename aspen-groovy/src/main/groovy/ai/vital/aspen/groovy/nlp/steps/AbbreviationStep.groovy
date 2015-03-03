package ai.vital.aspen.groovy.nlp.steps


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.domain.Abbreviation;
import ai.vital.domain.AbbreviationInstance;
import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasAbbreviation;
import ai.vital.domain.Edge_hasAbbreviationInstance;
import ai.vital.domain.Edge_hasSentenceAbbreviationInstance;
import ai.vital.domain.Sentence;
import ai.vital.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.model.DocumentUtils;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.vitalsigns.global.GlobalHashTable;
import ai.vital.vitalsigns.model.VITAL_Container;
import ai.vital.aspen.groovy.ontology.VitalOntology


class AbbreviationStep {

	
	public final static String ABBREVIATIONS_VS = "abbreviations_vs";
	
	private Logger log = LoggerFactory.getLogger(AbbreviationStep.class);

	/** The type asserted for extracted acronyms */
	public static final String SHORT_FORM_TYPE = "abbrevShort";
	/** The type asserted for expansions of extracted acronyms */
	public static final String LONG_FORM_TYPE = "abbrevLong";
	/** This property links an extracted acronym to its expansion */
	public static final String LONG_FORM_PROP = "expansion";
	/** This property links an expansion to its acronym */
	public static final String SHORT_FORM_PROP = "acronym";

	private Map<String,Vector<String>> mTestDefinitions = new HashMap<String,Vector<String>>();
	private int truePositives = 0, falsePositives = 0, falseNegatives = 0, trueNegatives = 0;
	private static final char DELIMITER = '\t';
	private boolean testMode = false;
  
	
	List<StringSpan> accum = new ArrayList<StringSpan>();
	boolean annotationMode = false;
	

	public String getName() {
		return ABBREVIATIONS_VS;
	}
	
	
	public void processPayload(VITAL_Container payload)
			
			throws Exception {

		annotationMode = true;
		
		for( Document doc : payload.iterator(Document.class) ) {
			
			accum.clear();
			
			String uri = doc.getURI();
			
			log.debug("Processing doc: {}", uri);

			String content = DocumentUtils.getTextBlocksContent(doc);
			
			int tbindex = 1;
			
			List tbs = doc.getTextBlocks();
			
			
			List<Abbreviation> currentAbbreviations = new ArrayList<Abbreviation>();
			
			for(TextBlock b : tbs) {
				
				long start = System.currentTimeMillis();
				List sentences = b.getSentences();
				long stop = System.currentTimeMillis();
				
				log.info("Processing tb: " + tbindex++ + " of " + tbs.size() + " with " + sentences.size() + " sentences, " + (stop-start) + "ms - HASH SIZE: " + GlobalHashTable.get().size());
				
				String blockText = b.text;
				
				for(Sentence s : sentences) {
					
					StringSpan sentSpan = new StringSpan(blockText.substring(s.startPosition, s.endPosition), 0, s.endPosition - s.startPosition);
					
					accum.clear();
					
					extractAbbrPairsFromSentence(sentSpan);

					for (Iterator<StringSpan> j=accum.iterator(); j.hasNext(); ) {
					
						StringSpan shortForm = j.next();
						
						StringSpan longForm = j.next();
						
						AbbreviationInstance ai = new AbbreviationInstance();
						
						String longFormText = longForm.asString();
						
						ai.longForm = longFormText;
						ai.longFormEnd = longForm.hi;
						ai.longFormStart = longForm.lo;
						
						String shortFormText = shortForm.asString();
						
						ai.shortForm = shortFormText;
						ai.shortFormEnd = shortForm.hi;
						ai.shortFormStart = shortForm.lo;
						
						log.debug("shortSpan["+shortForm.lo+".."+shortForm.hi+"] of doc: near '"+
								  content.substring(shortForm.lo,shortForm.hi)+"'");
						log.debug("shortForm='" + shortFormText + "' longForm='"+ longFormText + "'");
					
						
						
						Abbreviation parentA = null;
						
						for( Abbreviation a : currentAbbreviations ) {
							
							if(a.shortForm.equals(ai.shortForm) && a.longForm.equals(ai.longForm)) {
								
								parentA = a;
								break;
								
							}
							
						}
						
						List<AbbreviationInstance> abbreviationInstances = null;
						
						if(parentA != null) {
							
							abbreviationInstances = parentA.getAbbreviationInstances(payload);
							
						} else {
							
							parentA = new Abbreviation();
							parentA.shortForm = shortFormText;
							parentA.longForm = longFormText;
							abbreviationInstances = new ArrayList<AbbreviationInstance>();
//							parentA.setAbbreviationInstances(abbreviationInstances);
//							doc.getAbbreviations().add(parentA);
							parentA.setURI(doc.getURI() + "#abbreviation_" + ( currentAbbreviations.size() + 1) );
							
							//new parent abbreviations, link it to document
							payload.putGraphObjects(Arrays.asList(parentA));
							payload.putGraphObjects( EdgeUtils.createEdges(doc, Arrays.asList(parentA), Edge_hasAbbreviation, VitalOntology.Edge_hasAbbreviationURIBase) );
							
							currentAbbreviations.add(parentA);
							
						}
						
						abbreviationInstances.add(ai);
						ai.setURI(parentA.getURI() + "_instance_" + abbreviationInstances.size());
						
						payload.putGraphObjects(Arrays.asList(ai));
						
						//link abbrev instance to sentence and parent A
						payload.putGraphObjects( EdgeUtils.createEdges(parentA, Arrays.asList(ai), Edge_hasAbbreviationInstance, VitalOntology.Edge_hasAbbreviationInstanceURIBase) );
						payload.putGraphObjects( EdgeUtils.createEdges(s, Arrays.asList(ai), Edge_hasSentenceAbbreviationInstance, VitalOntology.Edge_hasSentenceAbbreviationInstanceURIBase) );
						
					}
					
				}
				
			}
			
		}
		
	}
	
	  
  private boolean isValidShortForm(String str) {
	return (hasLetter(str) && (Character.isLetterOrDigit(str.charAt(0)) || (str.charAt(0) == '(')));
  }

  private boolean hasLetter(String str) {
	for (int i=0; i < str.length() ; i++)
		if (Character.isLetter(str.charAt(i)))
		return true;
	return false;
  }

  private boolean hasCapital(String str) {
	for (int i=0; i < str.length() ; i++)
		if (Character.isUpperCase(str.charAt(i)))
		return true;
	return false;
  }

  private void loadTrueDefinitions(String inFile) {
	String abbrString, defnString, str = "";
	Vector<String> entry;
	Map<String,Vector<String>> definitions = mTestDefinitions;

	try {
		BufferedReader fin = new BufferedReader(new FileReader (inFile));
		while ((str = fin.readLine()) != null) {
		int j = str.indexOf(DELIMITER);
		abbrString = str.substring(0,j).trim();
		defnString = str.substring(j,str.length()).trim();
		entry = definitions.get(abbrString);
		if (entry == null)
		  entry = new Vector<String>();
		entry.add(defnString);
			definitions.put(abbrString, entry);
		}
	} catch (Exception e) {
		e.printStackTrace();
		System.out.println(str);
	}
  }
	
  private boolean isTrueDefinition(String shortForm, String longForm) {
	Vector<String> entry;
	Iterator<String> itr;

	entry = mTestDefinitions.get(shortForm);
	if (entry == null)
		return false;
	itr = entry.iterator();
	while(itr.hasNext()){
		if (itr.next().toString().equalsIgnoreCase(longForm))
		return true;
	}
	return false;
  }

  
 

  /** Holds a substring of a longer string. */
  public static class StringSpan
  {
	static public final StringSpan EMPTY = new StringSpan("",0,0);
	String base;
	int lo,hi;
	String mySubstring;
	public StringSpan(String b,int lo,int hi)
	{
	  this.base=b; this.lo=lo; this.hi=hi;
	  mySubstring = base.substring(lo,hi);
	}
	public StringSpan(StringSpan ss,int lo,int hi)
	{
	  this.base=ss.base; this.lo=ss.lo+lo; this.hi=ss.lo+hi;
	  this.mySubstring = this.base.substring(this.lo,this.hi);
	}
	public int offset() { return lo; }
	public int length() { return hi-lo; }
	public char charAt(int i) { return mySubstring.charAt(i); }
	public int indexOf(char ch) { return mySubstring.indexOf(ch); }
	public int indexOf(char ch,int fromIndex) { return mySubstring.indexOf(ch,fromIndex); }
	public int indexOf(String ch,int fromIndex) { return mySubstring.indexOf(ch,fromIndex); }
	public int indexOf(String s) { return mySubstring.indexOf(s); }
	public int lastIndexOf(String s) { return mySubstring.lastIndexOf(s); }
	public int lastIndexOf(String s,int fromIndex) { return mySubstring.lastIndexOf(s,fromIndex); }
	public String asString() { return mySubstring; }
	public StringSpan substring(int newLo,int newHi) { return new StringSpan(base,lo+newLo,lo+newHi); }
	public StringSpan substring(int newLo) { return new StringSpan(base,lo+newLo,hi); }
	public StringSpan trim()
	{
	  StringSpan ss = new StringSpan(base,lo,hi);
	  while (ss.lo<ss.hi && Character.isWhitespace(ss.base.charAt(ss.lo))) ss.lo++;
	  while (ss.hi>ss.lo && Character.isWhitespace(ss.base.charAt(ss.hi-1))) ss.hi--;
	  ss.mySubstring = ss.base.substring(ss.lo,ss.hi);
	  return ss;
	}
  }

  public void extractAbbrPairsFromSentence(StringSpan currSentence)
  {
	StringSpan longForm = StringSpan.EMPTY;
	StringSpan shortForm = StringSpan.EMPTY;
	int openParenIndex, closeParenIndex = -1, sentenceEnd, newCloseParenIndex, tmpIndex = -1;
	StringTokenizer shortTokenizer;

	log.debug("finding pairs in '"+currSentence.asString()+"'");
	openParenIndex =  currSentence.indexOf(" (");
	
	boolean firstRun = true;
	
	//first Run replaces do {} while () conditional
	
	while ( firstRun || ( (openParenIndex =  currSentence.indexOf(" (")) > -1 ) ) {
		
		firstRun = false;
		
	  if (openParenIndex > -1)
		openParenIndex++;
	  sentenceEnd = Math.max(currSentence.lastIndexOf(". "), currSentence.lastIndexOf(", "));
	  if ((openParenIndex == -1) && (sentenceEnd == -1)) {
		//Do nothing
	  }
	  else if (openParenIndex == -1) {
		currSentence = currSentence.substring(sentenceEnd + 2);
	  } else if ((closeParenIndex = currSentence.indexOf(')',openParenIndex)) > -1){
		sentenceEnd = Math.max(currSentence.lastIndexOf(". ", openParenIndex),
							   currSentence.lastIndexOf(", ", openParenIndex));
		if (sentenceEnd == -1)
		  sentenceEnd = -2;
		//longForm = currSentence.substring(sentenceEnd + 2, openParenIndex);
		//shortForm = currSentence.substring(openParenIndex + 1, closeParenIndex);
		longForm = new StringSpan(currSentence,sentenceEnd+2,openParenIndex);
		shortForm = new StringSpan(currSentence,openParenIndex+1,closeParenIndex);
	  }
	  if (shortForm.length() > 0 || longForm.length() > 0) {
		if (shortForm.length() > 1 && longForm.length() > 1) {
		  if ((shortForm.indexOf('(') > -1) &&
			  ((newCloseParenIndex = currSentence.indexOf(')', closeParenIndex + 1)) > -1)){
			//shortForm = currSentence.substring(openParenIndex + 1, newCloseParenIndex);
			shortForm = new StringSpan(currSentence, openParenIndex + 1, newCloseParenIndex);
			closeParenIndex = newCloseParenIndex;
		  }
		  if ((tmpIndex = shortForm.indexOf(", ")) > -1)
			shortForm = shortForm.substring(0, tmpIndex);
		  if ((tmpIndex = shortForm.indexOf("; ")) > -1)
			shortForm = shortForm.substring(0, tmpIndex);
		  shortTokenizer = new StringTokenizer(shortForm.asString());
		  if (shortTokenizer.countTokens() > 2 || shortForm.length() > longForm.length()) {
			// Long form in ( )
			tmpIndex = currSentence.lastIndexOf(" ", openParenIndex - 2);
			//tmpStr = currSentence.substring(tmpIndex + 1, openParenIndex - 1);
			StringSpan tmpStr = new StringSpan(currSentence,tmpIndex + 1, openParenIndex - 1);
			longForm = shortForm;
			shortForm = tmpStr;
			if (! hasCapital(shortForm.asString()))
			  shortForm = StringSpan.EMPTY;
		  }
		  if (isValidShortForm(shortForm.asString())){
			extractAbbrPair(shortForm.trim(), longForm.trim());
		  }
		}
		currSentence = currSentence.substring(closeParenIndex + 1);
	  } else if (openParenIndex > -1) {
		if ((currSentence.length() - openParenIndex) > 200)
		  // Matching close paren was not found
		  currSentence = currSentence.substring(openParenIndex + 1);
		break; // Read next line
	  }
	  shortForm = StringSpan.EMPTY;
	  longForm = StringSpan.EMPTY;
	}
  }

  private StringSpan findBestLongForm(StringSpan shortForm, StringSpan longForm)
  {
	int sIndex;
	int lIndex;
	char currChar;

	sIndex = shortForm.length() - 1;
	lIndex = longForm.length() - 1;
	for ( ; sIndex >= 0; sIndex--) {
		currChar = Character.toLowerCase(shortForm.charAt(sIndex));
		if (!Character.isLetterOrDigit(currChar))
		continue;
		while (((lIndex >= 0) && (Character.toLowerCase(longForm.charAt(lIndex)) != currChar)) ||
			 ((sIndex == 0) && (lIndex > 0) && (Character.isLetterOrDigit(longForm.charAt(lIndex - 1)))))
		lIndex--;
		if (lIndex < 0)
		return null;
		lIndex--;
	}
	lIndex = longForm.lastIndexOf(" ", lIndex) + 1;
	return longForm.substring(lIndex);
  }

  private void extractAbbrPair(StringSpan shortForm, StringSpan longForm)
  {
	StringSpan bestLongForm;
	StringTokenizer tokenizer;
	int longFormSize, shortFormSize;

	log.debug("finding long form for '"+shortForm.asString()+"' and '"+longForm.asString()+"'");

	if (shortForm.length() == 1)
		return;
	bestLongForm = findBestLongForm(shortForm, longForm);
	if (bestLongForm == null)
		return;
	tokenizer = new StringTokenizer(bestLongForm.asString(), " \t\n\r\f-");
	longFormSize = tokenizer.countTokens();
	shortFormSize = shortForm.length();
	for (int i=shortFormSize - 1; i >= 0; i--)
		if (!Character.isLetterOrDigit(shortForm.charAt(i)))
		shortFormSize--;
	if (bestLongForm.length() < shortForm.length() ||
		bestLongForm.indexOf(shortForm.asString() + " ") > -1 ||
		bestLongForm.asString().endsWith(shortForm.asString()) ||
		longFormSize > shortFormSize * 2 ||
		longFormSize > shortFormSize + 5 ||
		shortFormSize > 10)
		return;

	// at this point we have bestLongForm as expansion of shortForm
	if (annotationMode) {
	  accum.add( shortForm );
	  accum.add( bestLongForm );
	}

	if (testMode) {
		if (isTrueDefinition(shortForm.asString(), bestLongForm.asString())) {
		System.out.println(shortForm.asString() + DELIMITER + bestLongForm.asString() + DELIMITER + "TP");
		truePositives++;
		}
		else {
		falsePositives++;
		System.out.println(shortForm.asString() + DELIMITER + bestLongForm.asString() + DELIMITER + "FP");
		}
	} else if (!annotationMode) {
		System.out.println(shortForm.asString() + DELIMITER + bestLongForm.asString());
	}
  }


  
	
	
	
	
}
