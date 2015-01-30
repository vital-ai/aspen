/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.models;

import java.util.HashMap;
import java.util.Map;

import org.knallgrau.utils.textcat.TextCategorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LanguageDetector {

	private final static Logger log = LoggerFactory.getLogger(LanguageDetector.class);
	
	private static TextCategorizer singleton;
	
	private LanguageDetector() {		
	}

	public static void init()  {
		
		if(singleton == null) {
			
			synchronized(LanguageDetector.class) {
				
				if(singleton == null) {
					
					log.info("Initializing Language Detector ...");
					
					long start = System.currentTimeMillis();
					
					singleton = new TextCategorizer();
					
					long stop = System.currentTimeMillis();
					
					log.info("Language Detector loaded, {}ms", stop - start);
					
				}
				
			}
			
		} else {
			log.warn("Language Detector already initialized!");
		}
		
	}
	
	public static TextCategorizer getLanguageDetector()  {
		if(singleton == null) throw new Exception("Language Detector not initialized!");
		return singleton;
	}
	
	static Map<String, String> name2id = new HashMap<String, String>();
	
	static {
		String[] a = [
			"german", "de",
			"english", "en",
			"french", "fr",
			"spanish", "es",
			"italian", "it",
			"swedish", "se",
			"swedish", "se",
			"polish", "pl",
			"dutch", "nl",
			"norwegian", "no",
			"finnish", "fi",
			"albanian", "al",
			"slovakian", "sk",
			"slovenian", "si",
			"danish", "dk",
			"hungarian", "hu"
		] ;
		
		for(int i = 0 ; i < a.length; i+=2) {
			name2id.put(a[i], a[i+1]);
		}
		
	}
	
	public static String toLanguageID(String language) {
		String id = name2id.get(language);
		if(id != null) return id;
		return language;
	}
	
}
