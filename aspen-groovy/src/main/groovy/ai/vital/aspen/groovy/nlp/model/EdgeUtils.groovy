package ai.vital.aspen.groovy.nlp.model

import java.text.DecimalFormat
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

import ai.vital.vitalsigns.model.VITAL_Edge
import ai.vital.vitalsigns.model.VITAL_Node

class EdgeUtils {

	static DecimalFormat df = new DecimalFormat("000000");
	
	static Random random = new Random();
	
	public static List<VITAL_Edge> createEdges(VITAL_Node srcNode, List<VITAL_Node> destNodes, Class<? extends VITAL_Edge> edgeClass, String edgeURIBase) {

		List<VITAL_Edge> res = new ArrayList<VITAL_Edge>();
		
		int index = 0;
		
		for(VITAL_Node destNode : destNodes) {
			
			VITAL_Edge edge = edgeClass.newInstance();
			
			String edgeURI = edgeURIBase + System.currentTimeMillis() + "_" + index + df.format(random.nextInt(1000000));  //URIUtils.localURIPart(srcNode.getURI()) + "-to-" + URIUtils.localURIPart(destNode.getURI());
			
			edge.setURI(edgeURI);
			
			edge.setIndex(index++);
			
			edge.setSourceURI(srcNode.getURI());

			edge.setDestinationURI(destNode.getURI());
						
			res.add(edge);
			
		}
		
		return res;
				
	}
	
}
