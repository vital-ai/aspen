package ai.vital.aspen.groovy.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertBlockToSequenceTask;
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask;
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask;
import ai.vital.aspen.groovy.data.tasks.SegmentImportTask;
import ai.vital.aspen.groovy.task.AbstractTask;

public class SegmentImportProcedure {

    public List<String> inputPaths;
    
    public Map<String, Object> paramsMap;

    public String segmentID;

    private boolean overwrite;
    
    public SegmentImportProcedure(List<String> inputPaths,
            String segmentID, boolean overwrite, Map<String, Object> paramsMap) {
        super();
        this.inputPaths = inputPaths;
        this.segmentID = segmentID;
        this.overwrite = overwrite;
        this.paramsMap = paramsMap;
    }

    public List<AbstractTask> generateTasks() {
        
        List<AbstractTask> tasks = new ArrayList<AbstractTask>();
        
        for(String inputPath : inputPaths) {
            
            if(inputPath.startsWith("name:")) {
                throw new RuntimeException("import from namedRDD not supported yet");
            }
            
            if(inputPath.startsWith("spark-segment:")) {
                throw new RuntimeException("import from spark-segment not supported yet");
            }
            
            CheckPathTask cpt = new CheckPathTask(inputPath, paramsMap);
            
            cpt.acceptDirectories = true;
            cpt.acceptFiles = true;
            cpt.mustExist = true;
            cpt.mustnotExist = false;
//            cpt.validFileExtensions = new String[]{".vital.csv", ".vital.csv.gz"};
//            cpt.singleDirectory = true;
            
            tasks.add(cpt);
            
        }
        
        int i = 0;
        
        for(String inputPath : inputPaths) {
            
            i++;
            
            String inputDatasetName = inputPath;
            
            if(inputPath.startsWith("name:")) {

                inputDatasetName = inputPath.substring(5);
                
                paramsMap.put(inputDatasetName, true);
                
                tasks.add(new LoadDataSetTask(paramsMap, inputPath, inputDatasetName));
                
            } else if(inputPath.startsWith("spark-segment:")) {
                
                tasks.add(new LoadDataSetTask(paramsMap, inputPath, inputDatasetName));
                
            } else if(inputPath.endsWith(".vital.seq")) {
                
                tasks.add(new LoadDataSetTask(paramsMap, inputPath, inputDatasetName));
                
            } else if(inputPath.endsWith(".vital") || inputPath.endsWith(".vital.gz")) {
                
                throw new RuntimeException("vital block files direct import not supported yet"); 
                //convert it into sequence file first
//                String datasetSequencePath = AspenGroovyConfig.get().datesetsLocation + "/" + inputDatasetName + "/" + inputDatasetName + ".vital.seq";
//                
//                tasks.add(new DeletePathTask(datasetSequencePath, paramsMap));
//
//                tasks.add(new ConvertBlockToSequenceTask(Arrays.asList(inputPath), datasetSequencePath, paramsMap));
//                
//                tasks.add(new LoadDataSetTask(paramsMap, datasetSequencePath, inputDatasetName));
                
            } else if(inputPath.endsWith(".vital.csv") || inputPath.endsWith(".vital.csv.gz")) {
                
                // vital csv are loaded directly 
                
            } else {
                throw new RuntimeException("Unhandled input path URI: " + inputPath);
            }
            
        }
        

        //always check if segment exists
        CheckPathTask cpt = new CheckPathTask("spark-segment:" + segmentID, paramsMap);
        cpt.mustExist = true;
        cpt.mustnotExist = false;
        cpt.acceptFiles = true;
        cpt.acceptDirectories = true;
        cpt.validFileExtensions = new String[]{};
        cpt.singleDirectory = true;
        
        tasks.add(cpt);
            
        
        tasks.add(new SegmentImportTask(inputPaths, segmentID, overwrite, paramsMap));
        
        return tasks;

    }
    
}
