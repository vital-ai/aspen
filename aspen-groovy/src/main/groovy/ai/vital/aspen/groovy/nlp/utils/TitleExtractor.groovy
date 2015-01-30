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
