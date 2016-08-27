package ai.vital.aspen.groovy.taxonomy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.List;

public class Histogram<T> extends HashMap<T, Integer> {

	private static final long serialVersionUID = 5906293214698224711L;

	public void increment(T key, int delta) {
		
		Integer v = get(key);
		
		if(v == null) v = 0;
		
		v+= delta;
		
		put(key, v);
		
	}
	
	public void increment(T key) {
		increment(key, 1);
	}

	public void print(String title, final boolean asc) {

		List<Entry<T, Integer>> entries = new ArrayList<Entry<T, Integer>>(entrySet());
		
		Collections.sort(entries, new Comparator<Entry<T, Integer>>(){

			@Override
			public int compare(java.util.Map.Entry<T, Integer> o1,
					java.util.Map.Entry<T, Integer> o2) {

				int c = ( asc ? 1 : -1 ) * o1.getValue().compareTo(o2.getValue());
				
				return c;
			}
			
		});
		
		System.out.println(title  + " [" + entries.size() + "]");
		
		for(Entry<T, Integer> e : entries) {
			System.out.println(e.getKey() + " -> " + e.getValue());
		}
		
	}
	
	

}
