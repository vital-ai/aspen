package ai.vital.aspen.groovy.featureextraction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import ai.vital.predictmodel.Taxonomy;
import ai.vital.vitalsigns.model.VITAL_Category;
import ai.vital.vitalsigns.model.VITAL_Container;

public class CategoricalFeatureData extends FeatureData {

	private static final long serialVersionUID = 1L;

	//ordered list of categories
	private List<String> categories;

	public List<String> getCategories() {
		return categories;
	}

	public void setCategories(List<String> categories) {
		this.categories = categories;
	}

	@Override
	public Map<String, Object> toJSON() {
		Map m = new HashMap<String, Object>();
		m.put("categories", categories);
		return m;
	}

	@Override
	public void fromJson(Map<String, Object> unwrapped) {
		this.categories = (List<String>) unwrapped.get("categories");
	}
	
	/**
	 * helper method that creates a list of categories URI sorted by URI for given taxonomy
	 * taxonomy container must be set
	 * @return
	 */
	public static CategoricalFeatureData fromTaxonomy(Taxonomy taxonomy) {
		
		CategoricalFeatureData cfd = new CategoricalFeatureData();
		VITAL_Category rootCategory = taxonomy.getRootCategory();
		if(rootCategory == null) throw new NullPointerException("taxonomy " + taxonomy.getProvides() + " root category not set");
		VITAL_Container container = taxonomy.getContainer();
		if(container == null) throw new NullPointerException("taxonomy " + taxonomy.getProvides() + " container not set");
		List<String> cats = new ArrayList<String>();
		for( Iterator<VITAL_Category> iterator = container.iterator(VITAL_Category.class); iterator.hasNext(); ) {
			String uri = iterator.next().getURI();
			if(! rootCategory.getURI().equals(uri)) {
				cats.add(uri);
			}
		}
		
		Collections.sort(cats);

		cfd.setCategories(cats);
		
		return cfd;
		
	}
	
}
