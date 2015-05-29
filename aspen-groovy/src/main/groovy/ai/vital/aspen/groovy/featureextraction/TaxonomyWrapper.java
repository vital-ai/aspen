package ai.vital.aspen.groovy.featureextraction;

import java.io.Serializable;
import java.util.Iterator;

import ai.vital.predictmodel.Taxonomy;
import ai.vital.vitalsigns.model.VITAL_Category;
import ai.vital.vitalsigns.model.property.IProperty;

public class TaxonomyWrapper implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public Taxonomy taxonomy;

	public TaxonomyWrapper(Taxonomy taxonomy) {
		super();
		this.taxonomy = taxonomy;
	}

	public String URIforName(String name) {
		
		for( Iterator<VITAL_Category> iterator = taxonomy.getContainer().iterator(VITAL_Category.class); iterator.hasNext(); ) {
			
			VITAL_Category cat = iterator.next();
			IProperty nameP = (IProperty) cat.getProperty("name");
			if(nameP != null && nameP.toString().equals(name)) return cat.getURI(); 
		}
		
		return null;
	}
}
