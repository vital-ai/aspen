
package ai.vital.opennlp.classifier;

import java.io.InputStream;
import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;


public class Classifier {
     
    DoccatModel m;
         
    DocumentCategorizerME categorizer;
    
    int num_cat = 0;
    
    
    public void init(InputStream modelInputStream) throws java.io.IOException {
        
        m = new DoccatModel(modelInputStream);
        
        categorizer = new DocumentCategorizerME(m);
        
        num_cat = categorizer.getNumberOfCategories();
        
    }
    
    
    
    public Classification categorize(String input) {
        
    	if(categorizer == null) throw new RuntimeException("Classifier not initialized: use init(InputStream) first.");
        
        double[] outcomes = categorizer.categorize(input);
        
        String cat = categorizer.getBestCategory(outcomes);
        
        int i = categorizer.getIndex(cat);
        
        double score = outcomes[i];
        
        Classification c = new Classification(cat, score);
                
        return c;
        
    }
    
    public Classification[] categorizeDetailed(String[] sentence) {
    	
    	if(categorizer == null) throw new RuntimeException("Classifier not initialized: use init(InputStream) first.");
    	
    	double[] outcomes = categorizer.categorize(sentence);
    	
    	Classification[] res = new Classification[outcomes.length];
    	
    	for(int i = 0 ; i < outcomes.length; i++) {
    		
    		res[i] = new Classification(categorizer.getCategory(i), outcomes[i]);
    		
    	}
    	
    	return res;
    	
    	
    }
        
}
