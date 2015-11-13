package ai.vital.aspen.groovy.nlp.model

import com.vitalai.domain.nlp.TextBlock
import java.util.ArrayList;
import java.util.List;

class TransformationVectorUtils {

	public static void setTransformationVector(TextBlock textBlock, int[] tv) {
		
		if(tv != null) {
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < tv.length; i++) {
				if(sb.length() > 0) {
					sb.append(',');
				}
				sb.append(tv[i]);
			}
			textBlock.transformationVector = sb.toString();
			
		} else {
		
			textBlock.tranformationVector = null;
			
		}
		
	}
	
	public static int[] getTransformationVector(TextBlock textBlock) {
		
		List<Integer> vector = new ArrayList<Integer>();
		
		String vstring = textBlock.transformationVector
		
		if(vstring != null) {
			String[] split = vstring.split(",");
			for(String _s : split) {
				if(!_s.isEmpty()) {
					vector.add(Integer.parseInt(_s));
				}
			}
		}
		
		int[] va = new int[vector.size()];
		for(int i = 0; i < vector.size(); i++) {
			va[i] = vector.get(i);
		}

		return va;
				
	}
	
}
