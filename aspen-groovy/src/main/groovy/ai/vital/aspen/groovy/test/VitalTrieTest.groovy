package ai.vital.aspen.groovy.test

import ai.vital.aspen.utils.trie.VitalTrie

class VitalTrieTest {

	static main(args) {
		
		
		VitalTrie trie = new VitalTrie()
		
		
		trie.put("Awesome", "value")
		trie.put("Awe", "some")
		trie.put("awesome", "lowercase value")
		trie.put("different", "keyValuePair")
		trie.put("Awesome Key", "with an Awesome Value")
		trie.put("A", "B")
		
		SortedMap<String, String> prefixMap = trie.prefixMap("A")
		
		printPrefixMap(prefixMap)

		println ""

		SortedMap<String, String> prefixMap2 = trie.prefixMap("Awe")
		
		printPrefixMap(prefixMap2)
		
		
	}

	static void printPrefixMap(SortedMap<String, String> prefixMap) {
		
		 println "-------" 
		 
		for (Map.Entry<String, String> entry : prefixMap.entrySet()) {
			
			println "Key: <" + entry.getKey() + ">, Value: <" + entry.getValue() + ">"
		}
		
		println "-------"
	}
	
}
