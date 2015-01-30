package ai.vital.aspen.groovy.nlp.boilerpipe;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.l3s.boilerpipe.BoilerpipeFilter;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextBlock;
import de.l3s.boilerpipe.document.TextDocument;

public class CommentsSectionsFilter implements BoilerpipeFilter {

	private static CommentsSectionsFilter instance = new CommentsSectionsFilter();
	
	public static CommentsSectionsFilter getInstance() {
		return instance;
	}
	
	static Set<String> classes = new HashSet<String>(Arrays.asList(
		"comments", 
		"comment",
		"ysc-promos",
		"yom-secondary"
//		"yom-socialchrome-login"
		));
	
	static Set<String> classesSuffixes = new HashSet<String>(Arrays.asList(
		"-comments",
		"-comment"
	));
		
	static Set<String> identifiers = new HashSet<String>(Arrays.asList(
		"comments", 
		"commentsContainer",
		"dsq-content"));
	
	static Set<String> identifiersSuffixes = new HashSet<String>(Arrays.asList(
		"-comments"));
		
	@Override
	public boolean process(TextDocument doc)
		throws BoilerpipeProcessingException {

		boolean dirty = false;
			
		for( TextBlock tb : doc.getTextBlocks() ) {
				
			if(tb.isContent()) {
				
				List<String> parentClasses = tb.getParentClasses();
				
				for(String clsEl : parentClasses) {
					
					String[] classesArray = clsEl.split("\\s+");
						
					for(String cls : classesArray) {
							
						if(classes.contains(cls)) {
							//filter it out!
							tb.setIsContent(false);
							dirty = true;
						} else {
							
							for(String clsSuffix : classesSuffixes) {
								
								if(cls.endsWith(clsSuffix)) {
									tb.setIsContent(false);
									dirty = true;
								}
								
							}
							
						}
							
					}
					
				}
					
				for(String parentID : tb.getParentIDs() ) {
					
					if(identifiers.contains(parentID)) {
							
						tb.setIsContent(false);
							
						dirty = true;
							
					} else {
						
						for(String idSuffix : identifiersSuffixes) {
							
							if(parentID.endsWith(idSuffix)) {
								tb.setIsContent(false);
								dirty = true;
							}
							
						}
						
					}
					
				}
					
			}
				
		}
			
		return dirty;
	}
}
