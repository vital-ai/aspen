package ai.vital.aspen.model;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;

import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData;
import ai.vital.aspen.groovy.featureextraction.FeatureData;
import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData;
import ai.vital.aspen.groovy.featureextraction.TextFeatureData;
import ai.vital.aspen.groovy.featureextraction.WordFeatureData;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.VitalSignsDomainClassLoader;
import groovy.lang.GroovyClassLoader;

public class PredictionModelUtils {

	static List<Class<? extends FeatureData>> clazzToIndex = Arrays.asList(
		(Class<? extends FeatureData>)CategoricalFeatureData.class,
		WordFeatureData.class,
		NumericalFeatureData.class,
		TextFeatureData.class
	);
	
	/**
	 * Deserializes an object from bytes array.
	 * @param bytes
	 * @return
	 */
	public static <T> T deserialize(byte[] bytes) {
		
		ByteArrayInputStream is = new ByteArrayInputStream(bytes);
		
		return deserialize(is);
	
	}
	
	static class SpecialInputStream extends ObjectInputStream {
		
		public SpecialInputStream(InputStream arg0) throws IOException {
			super(arg0);
		}

		@Override
		protected Class<?> resolveClass(ObjectStreamClass arg0) throws IOException,
				ClassNotFoundException {

			Class<?> clz = null;
					
			try {
				return super.resolveClass(arg0);
			} catch(ClassNotFoundException e) {
			}
			
			try {
				return Thread.currentThread().getContextClassLoader().loadClass(arg0.getName());
			} catch(ClassNotFoundException e) {
			}

			try {
				return VitalSignsDomainClassLoader.get().loadClass(arg0.getName());
			} catch(ClassNotFoundException e) {
			}
			
			throw new ClassNotFoundException(arg0.getName());
			
		}

	}
	
	/**
	 * Deserializes a java object from input stream
	 * The stream will be closed once the object is written. This avoids the need for a finally clause, and maybe also exception handling, in the application code.
	 * @param inputStream
	 * @return
	 */
	public static <T> T deserialize(InputStream inputStream) {
		
		
		
		SpecialInputStream vois = null;
		try {
			vois = new SpecialInputStream(inputStream);
			return (T) vois.readObject();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(vois);
		}
	}
	
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
