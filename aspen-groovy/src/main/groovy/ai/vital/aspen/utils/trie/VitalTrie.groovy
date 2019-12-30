package ai.vital.aspen.utils.trie

import org.apache.commons.collections4.Trie
import org.apache.commons.collections4.trie.PatriciaTrie

class VitalTrie {

	
	// potentially use String, Double here for keeping a weight for the key
	// this may allow sorting by value to get the highest weight key given a prefix
	
	// weight can be used for frequency, so the most frequent (or important by some metric) phrases given a prefix can be retrieved
	
	
	
	Trie<String, String> trie = new PatriciaTrie<>()
	
	
	public void put(String key, String value) {
		
		trie.put(key, value)
		
	}
	
	
	public SortedMap<String, String> prefixMap(String prefix) {
		
		
		 return trie.prefixMap(prefix);

	}
	
}
