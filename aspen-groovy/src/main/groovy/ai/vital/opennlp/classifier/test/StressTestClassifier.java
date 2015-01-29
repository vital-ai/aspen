/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.opennlp.classifier.test;

import ai.vital.opennlp.classifier.Classification;
import ai.vital.opennlp.classifier.Classifier;

import java.io.FileInputStream;
import java.io.IOException;


public class StressTestClassifier {
 
    
    // preload all examples & get raw average classification score
    
    
    static int count = 1000000;
    
     static String test1 = "this movie totally sucked balls .";
    
    static String test2 = "this movie was totally awesome .";
    
    public static void main(String[] args) {
        
        Classifier classifier = new Classifier();
        try {
            
            
            classifier.init(new FileInputStream("scripts/en-sentiment.bin"));
            
            
            int x = 0;
            
            long start_time = System.nanoTime();
            
            while(x < count) {
            
            
            Classification c1 = classifier.categorize(test1);
            
            
            Classification c2 = classifier.categorize(test2);
            
            
         
            x++;
        }
            
         
            long stop_time = System.nanoTime();
            
            
            long delta = stop_time - start_time;
            
            double per_classify = ( (double) delta / (double) count ) / (double) 1000000.00 ;
            
            System.out.println("Milleseconds per classification: " + per_classify);
            
            
        } catch (IOException ex) {
            
            ex.printStackTrace();
           
        }
        
        
        
        
    }
    
    
}
