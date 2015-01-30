package ai.vital.opennlp.classifier.test;

import ai.vital.opennlp.classifier.Classification;
import ai.vital.opennlp.classifier.Classifier;

import java.io.FileInputStream;
import java.io.IOException;

public class TestClassifier {
    
    
    static String test1 = "this movie totally sucked balls .";
    
    static String test2 = "this movie was totally awesome .";
    
    public static void main(String[] args) {
        
        Classifier classifier = new Classifier();
        try {
            
            classifier.init(new FileInputStream("scripts/en-sentiment.bin"));
            
            
            Classification c1 = classifier.categorize(test1);
            
            System.out.println(c1.category + "(" + c1.score + ") " + test1);
            
            Classification[] c1D = classifier.categorizeDetailed(test1.split(" "));
            
            for(Classification c : c1D) {
            	System.out.println("\t" + c.category + " (" + c.score + ")");
            }
            
            System.out.println();
            
            Classification c2 = classifier.categorize(test2);
            
            System.out.println(c2.category + "(" + c2.score + ") " + test2);
            
            Classification[] c2D = classifier.categorizeDetailed(test2.split(" "));

            for(Classification c : c2D) {
            	System.out.println("\t" + c.category + " (" + c.score + ")");
            }
            
            
        } catch (IOException ex) {
            
            ex.printStackTrace();
           
        }
        
    }
    
    
}
