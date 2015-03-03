package ai.vital.flow.server.utils;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import ai.vital.vitalsigns.model.VITAL_Edge;
import ai.vital.vitalsigns.model.VITAL_Node;

public class EdgeUtils {

	static DecimalFormat df = new DecimalFormat("000000");
	
	static Random random = new Random();
	
	public static VITAL_Edge createEdge(VITAL_Node srcNode, VITAL_Node destNode, Class<? extends VITAL_Edge> edgeClass, String edgeURIBase) {
		return createEdges(srcNode, Arrays.asList(destNode), edgeClass, edgeURIBase).get(0);
	}
	
	public static List<VITAL_Edge> createEdges(VITAL_Node srcNode, List<VITAL_Node> destNodes, Class<? extends VITAL_Edge> edgeClass, String edgeURIBase) {

		List<VITAL_Edge> res = new ArrayList<VITAL_Edge>();
		
		int index = 0;
		
		for(VITAL_Node destNode : destNodes) {
			
			VITAL_Edge edge;
			try {
				edge = edgeClass.newInstance();
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
			
			String edgeURI = edgeURIBase + System.currentTimeMillis() + "_" + index + df.format(random.nextInt(1000000));  //URIUtils.localURIPart(srcNode.getURI()) + "-to-" + URIUtils.localURIPart(destNode.getURI());
			
			edge.setURI(edgeURI);
			
			edge.setProperty("listIndex", index++);
			
			edge.setSourceURI(srcNode.getURI());

			edge.setDestinationURI(destNode.getURI());
						
			res.add(edge);
			
		}
		
		return res;
				
	}
	
}
