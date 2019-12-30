package ai.vital.aspen.utils.trie

import org.apache.commons.collections4.Trie
import org.apache.commons.collections4.trie.PatriciaTrie

class VitalTrie {

	
	Trie<String, String> trie = new PatriciaTrie<>()
	
	
	public void put(String key, String value) {
		
		trie.put(key, value)
		
	}
	
	
	public SortedMap<String, String> prefixMap(String prefix) {
		
		
		 return trie.prefixMap(prefix);

	}
	
}
