
package ai.vital.opennlp.classifier.train;

import java.io.*;
import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.doccat.DocumentSample;
import opennlp.tools.doccat.DocumentSampleStream;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;


public class TrainClassifier {
    
    
    public static void main(String[] args) {
        
        
        String dir = "";
        
        
        DoccatModel model = null;

InputStream dataIn = null;
try {
  dataIn = new FileInputStream(dir + "en-sentiment.train");
  ObjectStream<String> lineStream =
		new PlainTextByLineStream(dataIn, "UTF-8");
  ObjectStream<DocumentSample> sampleStream = new DocumentSampleStream(lineStream);

  model = DocumentCategorizerME.train("en", sampleStream);
}
catch (IOException e) {

	e.printStackTrace();
}
finally {
  if (dataIn != null) {
    try {
      dataIn.close();
    }
    catch (IOException e) {
      
      e.printStackTrace();
    }
  }
}
   
System.out.println("Saving model...");

String modelFile = dir + "en-sentiment.bin";

OutputStream modelOut = null;
try {
  modelOut = new BufferedOutputStream(new FileOutputStream(modelFile));
  model.serialize(modelOut);
}
catch (IOException e) {

	e.printStackTrace();
}
finally {
  if (modelOut != null) {
    try {
       modelOut.close();
    }
    catch (IOException e) {
      
      e.printStackTrace();
    }
  }
}

    
    }
   
    
}
