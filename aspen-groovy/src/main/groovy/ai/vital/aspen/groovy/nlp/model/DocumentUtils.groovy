package ai.vital.aspen.groovy.nlp.model

import com.vitalai.domain.nlp.Document
import com.vitalai.domain.nlp.Sentence
import com.vitalai.domain.nlp.TextBlock

class DocumentUtils {

	public static String getTextBlocksContent(Document document) {

		StringBuilder sb = new StringBuilder();
		
		boolean first = true;
		
		for(TextBlock b : document.getTextBlocks()) {
			
			if(first) {
				first = false;
			} else {
				sb.append(' ');
			}
			
			sb.append((String)b.text);
			
			
		}
		
		return sb.toString();
		
	}

	
	public static TextBlock getTextBlockForOffset(Document doc, int offset) {
		
		//first we need to identify the block it is located in			
		int cursor = 0;
			
		int offsetWithinBlock = -1;
			
		for(TextBlock tb : doc.getTextBlocks()) {
				
			String text = tb.text
				
			int length = text.length();
				
			if(offset >= cursor && offset <= cursor + length) {
					
				offsetWithinBlock = offset - cursor;
					
				return tb;
					
			}
	
			//space!
			cursor = cursor + length + 1;
				
		}
			
		return null;
			
	}
		
	public static Integer translateBlocksContentOffsetToBodyOffset(Document doc, int offset) {
	
			
		//first we need to identify the block it is located in
			
		TextBlock b = null;
			
		int cursor = 0;
			
		int offsetWithinBlock = -1;
			
		for(TextBlock tb : doc.getTextBlocks()) {
				
			String text = tb.text
				
			int length = text.length();
			
			if(offset >= cursor && offset <= cursor + length) {
					
				offsetWithinBlock = offset - cursor;
					
				b = tb;
					
				break;
					
			}
	
			//space!
			cursor = cursor + length + 1;
				
		}
			
		if(b != null) {
				
			//now translate the block offset and also include the
	
			int accumulator = 0;
				
			int[] transformationVector = TransformationVectorUtils.getTransformationVector(b);
				
			int transformationVectorCursor = 0;
				
				
			for(int i = 0; i < offsetWithinBlock; i++) {
	
				int lastReadValue = transformationVector[transformationVectorCursor++];
					
				accumulator++;
					
				while(lastReadValue == 0) {
						
					lastReadValue = transformationVector[transformationVectorCursor++];
						
					accumulator++;
						
				}
					
			}
				
			return b.textBlockOffset + accumulator;
				
		} else {
			return offset;
		}
			
	}
	
	//return sentence, offset within sentence
	public static Object[] translateBlocksContentOffsetToSentenceOffset(Document doc, int offset) {
		
		//first we need to identify the block it is located in
		
		int cursor = 0;
		
		int offsetWithinBlock = -1;
		
		int offsetWithinSentence = -1;
		
		Sentence sentence = null;
		
		for( TextBlock tb : doc.getTextBlocks() ) {
			
			String text = tb.text
			
			int length = text.length();
			
			if(offset >= cursor && offset <= cursor + length) {
				
				offsetWithinBlock = offset - cursor;
				
				//localize the sentence
				for(Sentence s : tb.getSentences()) {
					
					int start = cursor + s.startPosition.rawValue();
					int end = cursor + s.endPosition.rawValue();
					
					if(offset >= start && offset <= end) {
						
						sentence = s;
						offsetWithinSentence = offset - start;
						break;
						
					}
					
				}
				
				break;
				
			}

			//space!
			cursor = cursor + length + 1;
			
		}
		
		return [sentence, offsetWithinSentence];
		
	}
		
}
