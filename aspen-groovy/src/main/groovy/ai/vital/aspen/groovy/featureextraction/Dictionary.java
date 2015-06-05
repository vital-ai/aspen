package ai.vital.aspen.groovy.featureextraction;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//dictionary used for text feature extraction
//contains words index ( Map<String,Integer ) 
public class Dictionary implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private Map<String, Integer> source;

	public Dictionary(Map<String, Integer> source) {
		this.source = source;
	}
	
	public Integer get(String word) {
		return this.source.get(word);
	}
	
	public String getReverse(Integer value) {
		
		for( Entry<String, Integer> e : this.source.entrySet()) {
			
			if(e.getValue().intValue() == value.intValue()) {
				return e.getKey();
			}
			
		}
		
		return null;
		
	}
	
	public int size() {
		return this.source.size();
	}
	
	public final static Pattern dictionaryPattern = Pattern.compile("(\\d+)\t(.+)");
	
	public void saveTSV(OutputStream outputStream) throws IOException {
		
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
		
		List<Entry<String, Integer>> entries = new ArrayList<Entry<String, Integer>>(source.entrySet());
		
		Collections.sort(entries, new Comparator<Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				return o1.getValue().compareTo(o2.getValue());
			}
		});
	
		for(Entry<String, Integer> e : entries) {
			writer.write(e.getValue() + "\t" + e.getKey() + "\n");
		}
		
		writer.flush();
		
	}
	
	/**
	 * Input stream is not closed
	 * @param inputStream
	 * @return
	 * @throws IOException 
	 */
	public static Dictionary fromTSV(InputStream inputStream) throws IOException {
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		
		Map<String, Integer> m = new HashMap<String, Integer>();
		
		BitSet control = new BitSet();
		
		int c = 0;
		
		for(String l = reader.readLine(); l != null; l = reader.readLine()) {
			
			c++;
			
			l = l.trim();
			
			if(l.isEmpty()) continue;
			
			Matcher matcher = dictionaryPattern.matcher(l.trim());
			
			if(!matcher.matches()) throw new IOException("Line: " + c + " - Expected a dictionary line pattern: \\d+\\t.*");
			
			Integer index = Integer.parseInt(matcher.group(1));
			
			String word = matcher.group(2);
			
			if( control.get(index) ) throw new IOException("Line: " + c + " - word index already in use: " + index);
			
			Integer previous = m.put(word, index);
			
			if(previous != null) throw new IOException("Line: " + c + " - a word appeared more than once: " + word);
			
			control.set(index, true);
			
//			m.put(key, value)
			
		}
		
		for(int i = 0 ; i < control.size(); i++) {
			if( ! control.get(i) ) throw new IOException("Dictionary index is not continuous, missing index " + i );
		}
		
		if(m.size() == 0) throw new IOException("Empty dictionary");
		
		Dictionary d = new Dictionary(m);
		
		return d;
		
	}
	
	public static Dictionary createDictionaryMaxPercent(Map<String, Integer> documentFrequency, Integer totalDocs, Integer minDocFreqInclusive, Integer maxDocPercent) {
		
		int maxDocFreqInclusive = (int) ( (double)totalDocs * (double) maxDocPercent / 100d );
		
		return createDictionary(documentFrequency, minDocFreqInclusive, maxDocFreqInclusive);
	}
	
	public static Dictionary createDictionary(Map<String, Integer> documentFrequency, Integer minDocFreqInclusive, Integer maxDocFreqInclusive) {
		
		//filter
		List<String> output = new ArrayList<String>();
		
		for(Entry<String, Integer> e : documentFrequency.entrySet() ) {
			
			if( ( minDocFreqInclusive == null || e.getValue() >= minDocFreqInclusive ) && (maxDocFreqInclusive != null &&  e.getValue() <= maxDocFreqInclusive) ) {
		
				output.add(e.getKey());
				
			}
			
		}
		
		Collections.sort(output);
		
		Map<String, Integer> m = new HashMap<String, Integer>();
		
		for(int i = 0 ; i < output.size(); i++) {
			
			m.put(output.get(i), i);
			
		}
		
		return new Dictionary(m);
		
	}

	public List<String> keysList() {

		List<String> l = new ArrayList<String>(source.keySet());
		
		Collections.sort(l, new Comparator<String>(){

			@Override
			public int compare(String w1, String w2) {
				return source.get(w1).compareTo(source.get(w2));
			}});
		
		return l;
		
	}
	
}
