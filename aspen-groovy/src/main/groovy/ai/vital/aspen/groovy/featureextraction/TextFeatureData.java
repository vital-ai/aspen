package ai.vital.aspen.groovy.featureextraction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TextFeatureData extends FeatureData {

	private static final long serialVersionUID = 1L;
	
	private Dictionary dictionary;

	public Dictionary getDictionary() {
		return dictionary;
	}

	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

	@Override
	public Map<String, Object> toJSON() {
		
		Map<String, Object> m = new HashMap<String, Object>();
		
		//persist dictionary as strings list
		m.put("dictionary", dictionary.keysList());
		
		return m;
		
	}

	@Override
	public void fromJson(Map<String, Object> m) {
		
		List<String> dictionary = (List<String>) m.get("dictionary");
		
		Map<String, Integer> x = new HashMap<String, Integer>();
		
		for(int i = 0; i < dictionary.size(); i++) {
			x.put(dictionary.get(i), i);
		}
		
		this.dictionary = new Dictionary(x);
		
		
	}
	
	
}

