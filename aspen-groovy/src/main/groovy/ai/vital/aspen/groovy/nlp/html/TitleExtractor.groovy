package ai.vital.aspen.groovy.nlp.html;

import java.util.ArrayList;
import java.util.List;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class TitleExtractor {

	public static String extractTitle(Document doc) {
		
		String extractedTitle = null;
		
		Element titleEl = doc.select("title").first();
		
		String htmlTitleLC = titleEl != null ? titleEl.text().trim().toLowerCase() : "";
		
		//select all header elements up to 3rd level ? 
		List<Element> headers = new ArrayList<Element>();
		headers.addAll(doc.select("h1"));
		headers.addAll(doc.select("h2"));
		headers.addAll(doc.select("h3"));

		if(!htmlTitleLC.isEmpty()) {
			
			//iterate over all header elements and compare with headers
			
			//take the longer common part
			
			String longestCommonPart = null;
			
			for(Element h : headers) {
				
				String header = h.text().trim();
				
				//for sanity title shorter than 10 chars will be ignored
				if( header.length() > 10 && htmlTitleLC.contains( header.toLowerCase() ) ) {
					
					if(longestCommonPart == null || longestCommonPart.length() < header.length()) {
						
						longestCommonPart = header;
						
					}
					
					
				}
				
				
			}
			
			if(longestCommonPart != null) {
				
				extractedTitle = longestCommonPart;
				
			}
			
			
		} else {
			
			if(headers.size() > 0) {
				String trim = headers.get(0).text().trim();
				if(trim.length() > 10) {
					extractedTitle = trim;
				}
			}
			
		}

		return extractedTitle;
		
	}
}
