package ai.vital.aspen.groovy.nlp.html;

import org.apache.commons.lang3.StringEscapeUtils;

import ai.vital.aspen.groovy.nlp.model.TransformationVectorUtils;
import com.vitalai.domain.nlp.TextBlock

public class HTMLTransformation_VS {

	public static void appendText(TextBlock input, String htmlText) {
		
		StringBuilder buffer = new StringBuilder(input.text);
		
		String transformationVector = input.transformationVector;
		
//		int[] tv = TransformationVectorUtils.getTransformationVector(input);
		
		for(int i = 0 ; i < htmlText.length(); i++) {
			
			char c = htmlText.charAt(i);
			
			if(c == '&') {
				
				//check if it's a known
				
				int indexOfNext  = htmlText.indexOf(';', i+1);

				if(indexOfNext > 0) {
					
					String entity = htmlText.substring(i, indexOfNext + 1);
					
					String unescapedString = StringEscapeUtils.unescapeHtml4(entity);
					
					if(unescapedString.length() == 1) {
						
						buffer.append(unescapedString);
						
						/*
						input.text = input.text + unescapedString;
						
						int[] tv = TransformationVectorUtils.getTransformationVector(input);
						
						tv = appendElement(tv, 1);
						*/
						
						if(transformationVector.length() > 0) {
							transformationVector += ',1';
						} else {
							transformationVector += '1';
						}
						
//						TransformationVectorUtils.setTransformationVector(input, tv);
						
//						input.setTransformationVector(appendElement(input.getTransformationVector(), 1));
						
						//mute the rest of the entity string
						for(int j = 1; j < entity.length(); j++) {
							
//							input.setTransformationVector(appendElement(input.getTransformationVector(),0));
							
							/*
							int[] tv_ = TransformationVectorUtils.getTransformationVector(input);
							
							tv_ = appendElement(tv_, 0);
							
							TransformationVectorUtils.setTransformationVector(input, tv_);
							*/
							
							if(transformationVector.length() > 0) {
								transformationVector += ',0';
							} else {
								transformationVector += '0';
							}
							
						}

						
						//move pointer, -1 loop incrementation
						i = i + entity.length() - 1 ;
						
						continue;
						
						
					}
					
					
					
				}
				
				
				
				
			}
			
			//just copy the character
//			input.text = input.text + c;
			buffer.append(c);
			
			/*
			int[] tv = TransformationVectorUtils.getTransformationVector(input);
			
			tv = appendElement(tv, 1);
			
			TransformationVectorUtils.setTransformationVector(input, tv);
			*/
			
			if(transformationVector.length() > 0) {
				transformationVector += ',1';
			} else {
				transformationVector += '1';
			}

						
//			input.setTransformationVector(appendElement(input.getTransformationVector(), 1));
			
			
		}
	
		
		input.text = buffer.toString();
		
		input.transformationVector = transformationVector; 
		
	}
	
	private static int[] appendElement(int[] inputEl, int newValue) {

		int[] res = new int[inputEl.length + 1];
		
		for(int i = 0; i < inputEl.length; i++) {
			res[i] = inputEl[i];
		}
		
		res[inputEl.length] = newValue;
		
		return res;
	}

	public static void appendMuted(TextBlock input, int length) {
		
		String transformationVector = input.transformationVector;
		
		for(int i = 0 ; i < length; i++) {
			/*
			int[] tv = TransformationVectorUtils.getTransformationVector(input);
			tv = appendElement(tv, 0);
			TransformationVectorUtils.setTransformationVector(input, tv);
			*/
			
//			input.setTransformationVector(appendElement(input.getTransformationVector(), 0));
			if( transformationVector.length() > 0 ) {
				transformationVector += ",0";
			} else {
				transformationVector += "0";
			}
		}
	
		input.transformationVector = transformationVector;
			
	}
	
	public static void main(String[] args) {
		
		TextBlock tb = new TextBlock();
		tb.text = "";
//		appendMuted(tb, 6);
		String inputHTML = "test &nbsp; &#0034; &gt;  &WRONG; &#1;";
		appendText(tb, inputHTML);
		
		String text = tb.text;
		
		System.out.println();
		
	}
	
}
