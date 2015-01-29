/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/

package ai.vital.opennlp.classifier;


public class Classification {
    
    public String category;
    
    public double score;
    
   
    public Classification(String category, double score) {
        
        this.category = category;
        this.score = score;
        
    }
    
    
}