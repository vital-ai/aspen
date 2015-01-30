package ai.vital.aspen.groovy.nlp.model

class URIUtils {

	public static String localURIPart(String uri) {
		return localURIPart(uri, false);
	}
	
	public static String localURIPart(String uri, boolean skipURLParams) {
	
		int lastHash = uri.lastIndexOf('#');
		
		int lastSlash = uri.lastIndexOf('/');
		
		int max = Math.max(lastHash, lastSlash);
		
		if(max < 0 || max >= uri.length()-1) {
			throw new RuntimeException("Couldn't extract local part from URI: " + uri);
		}
		
		uri = uri.substring(max + 1);
		
		if(skipURLParams) {
			
			int indexOf = uri.indexOf('?');
			
			if(indexOf > 0) {
				
				return uri.substring(0, indexOf);
				
			}
			
		}
		
		return uri;
		
	}
	
}
