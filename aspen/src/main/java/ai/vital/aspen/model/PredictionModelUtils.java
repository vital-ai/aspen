package ai.vital.aspen.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData;
import ai.vital.aspen.groovy.featureextraction.FeatureData;
import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData;
import ai.vital.aspen.groovy.featureextraction.TextFeatureData;

public class PredictionModelUtils {

	static List<Class<? extends FeatureData>> clazzToIndex = Arrays.asList(
		(Class<? extends FeatureData>)CategoricalFeatureData.class,
		NumericalFeatureData.class,
		TextFeatureData.class
	);
	
	public static List<Entry<String, FeatureData>> getOrderedFeatureData(Map<String, FeatureData> features) {
		
		List<Entry<String, FeatureData>> list = new ArrayList<Entry<String, FeatureData>>(features.entrySet());
		
		Collections.sort(list, new Comparator<Entry<String, FeatureData>>() {

			@Override
			public int compare(Entry<String, FeatureData> o1,
					Entry<String, FeatureData> o2) {

				int i1 = clazzToIndex.indexOf(o1.getValue().getClass());
				if(i1 < 0) throw new RuntimeException("Feature data type unsupported: " + o1.getValue().getClass());
				int i2 = clazzToIndex.indexOf(o2.getValue().getClass());
				if(i2 < 0) throw new RuntimeException("Feature data type unsupported: " + o2.getValue().getClass());
				
				int c = new Integer(i1).compareTo(i2);
				
				if(c != 0) return c;
				
				return o1.getKey().compareTo(o2.getKey());
				
			}
		});
		
		return list;
	}
	
}
