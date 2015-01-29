/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.utils;

public class TitleExtractor {

	public static String extractTitle(String content) {

	   String lc = content.toLowerCase();
			   
	   int startTag = lc.indexOf("<title>");
		   
	   int endTag = lc.indexOf("</title>");
			   
	   if(startTag < 0) return null;
			   
	   if(endTag < 0) return null;
			   
	   if(startTag > endTag) return null;
			   
	   return content.substring(startTag + 7, endTag).trim();
			   
	}
	
}
