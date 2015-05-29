package ai.vital.aspen.groovy.featureextraction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sun.tools.jdi.LinkedHashMap;

import ai.vital.predictmodel.Aggregate;
import ai.vital.predictmodel.Function;
import ai.vital.predictmodel.PredictionModel;

/**
 * Analyzes the prediction model and determines the processing order of functions
 * @author Derek
 *
 */
public class PredictionModelAnalyzer {

	public static class OperationsOrder {
		
		public List<Aggregate> aggregates;
		
		public List<Function> functions;
		
	}
	
	public static void fixFunctionsAggregatesOrder(PredictionModel model) {
		
		OperationsOrder oo = getFunctionsOrder(model);
		
		model.setFunctions(oo.functions);
		model.setAggregates(oo.aggregates);
		
	}
	
	public static OperationsOrder getFunctionsOrder(PredictionModel model) {
		
		List<Function> functions = new ArrayList<Function>();
		if(model.getFunctions() != null ) functions.addAll(model.getFunctions());
		
		List<Aggregate> aggregates = new ArrayList<Aggregate>();
		if(model.getAggregates() != null) aggregates.addAll(model.getAggregates());

		List<Function> orderedFunctions = new ArrayList<Function>();
		List<Aggregate> orderedAggregates = new ArrayList<Aggregate>();
		
		//collect aggregate naems
		Set<String> aggregatesNames = new HashSet<String>();
		

		for(Aggregate a : aggregates) {
			aggregatesNames.add(a.getProvides());
		}
		
		while(aggregates.size() > 0) {
			
			for(Iterator<Aggregate> iter = aggregates.iterator(); iter.hasNext(); ) {
				
				Aggregate a = iter.next();
				
				boolean mayPut = true;
				
				if(a.getRequires() != null) {
					
					for(String r : a.getRequires()) {
						
						if(aggregatesNames.contains(r)) {
							
							//check if it is in top
							boolean alreadyStacked = false;
							
							for(Aggregate x : orderedAggregates) {
								
								if(x.getProvides().equals(r)) {
									alreadyStacked = true;
									break;
								}
								
							}
							
							if(!alreadyStacked) {
								mayPut = false;
								break;
							}
							
						}
						
					}
					
				}
				
				if(mayPut) {
					orderedAggregates.add(a);
					iter.remove();
				}
				
			}
			
		}
		
		while(functions.size() > 0) {
			
			for( Iterator<Function> iter = functions.iterator(); iter.hasNext(); ) {
				
				Function next = iter.next();
				
				boolean mayPut = true;
				
				if(next.getRequires() != null) {
					
					for(String req : next.getRequires()) {
						
						if(aggregatesNames.contains(req)) continue;
						
						//check if it is in top
						boolean alreadyStacked = false;
						
						for(Function x : orderedFunctions) {
							
							if(x.getProvides().equals(req)) {
								alreadyStacked = true;
								break;
							}
							
						}
						
						if(!alreadyStacked) {
							mayPut = false;
							break;
						}
						
					}
					
				}
				
				if(mayPut) {
					orderedFunctions.add(next);
					iter.remove();
				}
				
			}
			
			
		}
		
		OperationsOrder oo = new OperationsOrder();
		oo.aggregates = orderedAggregates;
		oo.functions = orderedFunctions;
		return oo;
		
	}
	
	public static List<Function> getAggregationFunctions(PredictionModel pm, Aggregate a) {
		
		Map<String, Function> r = new HashMap<String, Function>();
		
		Map<String, Function> fs = new HashMap<String, Function>();
		for( Function f : pm.getFunctions() ) {
			fs.put(f.getProvides(), f);
		}
		
		Set<String> requires = new HashSet<String>(a.getRequires());
		
		while(requires.size() > 0) {

			Set<String> newReq = new HashSet<String>();
			
			for(String req : requires) {
				
				Function function = fs.get(req);
				
				if(function == null) throw new RuntimeException("Aggregate " + a.getProvides() + " dependency not found: " + req);
				
				r.put(function.getProvides(), function);
				
				if(function.getRequires() != null) {
					for(String nr : function.getRequires()) {
						if(!r.containsKey(nr)) newReq.add(nr);
					}
				}
				
			}
			
			requires = newReq;
			
		}
		
		return new ArrayList<Function>(r.values());
		
	}
	
}
