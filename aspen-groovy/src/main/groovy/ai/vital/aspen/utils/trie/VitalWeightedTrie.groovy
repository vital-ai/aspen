package ai.vital.aspen.utils.trie

import java.util.SortedMap
import org.apache.commons.collections4.Trie
import org.apache.commons.collections4.trie.PatriciaTrie

class VitalWeightedTrie {

	
	
	Trie<String, Double> trie = new PatriciaTrie<>()
	
	
	public void put(String key, Double value) {
		
		trie.put(key, value)
		
	}
	
	
	public SortedMap<String, Double> prefixMap(String prefix) {
		
		// re-sort by weight here?
		
		 return trie.prefixMap(prefix);

	}
	
	
	
}
