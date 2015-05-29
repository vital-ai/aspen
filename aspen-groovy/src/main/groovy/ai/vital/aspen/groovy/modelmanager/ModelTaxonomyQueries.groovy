package ai.vital.aspen.groovy.modelmanager

import ai.vital.query.querybuilder.VitalBuilder;
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalsigns.model.Edge_hasChildCategory;
import ai.vital.vitalsigns.model.VITAL_Category;
import ai.vital.vitalsigns.model.property.URIProperty;

class ModelTaxonomyQueries {

	static VitalBuilder vitalBuilder = new VitalBuilder()
	
	public static VitalPathQuery getTaxonomyPathQuery(VITAL_Category rootCategory) {
		return getTaxonomyPathQuery(rootCategory.URI)	
	}
	
	public static VitalPathQuery getTaxonomyPathQuery(String rootCategoryURI) {
		
		VitalPathQuery vpq = vitalBuilder.query {
			
			PATH {
				
				value segments: '*'
				
				value maxdepth: '*'
				
				ARC {
					
					value direction: 'forward'
					
					edge_constraint { Edge_hasChildCategory.class }
					
					node_constraint { VITAL_Category.class }
					
				}
				
			}
			
		}.toQuery()
		
		vpq.setRootURIs([URIProperty.withString(rootCategoryURI)])
		
		return vpq
		
	}
	
}
