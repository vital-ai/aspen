package ai.vital.aspen.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A special type of map where each key can be set only once and no elements removed - 
 * appending operations are supported only
 * 
 * @author Derek
 *
 */
public class SetOnceHashMap extends HashMap<String, Object> {

	private static final long serialVersionUID = 1L;

	@Override
	public void clear() {
		throw new UnsupportedOperationException("This map cannot be purged");
	}

	@Override
	public Set<String> keySet() {
		return new HashSet<String>(super.keySet());
	}

	@Override
	public Object put(String arg0, Object arg1) {
		if(containsKey(arg0)) throw new RuntimeException("key already set: " + arg0);
		return super.put(arg0, arg1);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> arg0) {

		Set<String> ex = null;
		for(String k : arg0.keySet() ) {
			if(containsKey(k)) {
				if(ex == null) ex = new HashSet<String>();
				ex.add(k);
			}
		}
		
		if(ex != null) throw new RuntimeException("keys already set [" + ex.size() + "]: " + ex);
		
		super.putAll(arg0);
		
	}

	@Override
	public Object remove(Object arg0) {
		throw new UnsupportedOperationException("Elements from this map cannot be removed or replaced");
	}

	@Override
	public Collection<Object> values() {
		return new ArrayList<Object>(super.values());
	}
	
}
