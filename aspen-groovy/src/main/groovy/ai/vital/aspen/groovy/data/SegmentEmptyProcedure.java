package ai.vital.aspen.groovy.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.data.tasks.SegmentEmptyTask;
import ai.vital.aspen.groovy.task.AbstractTask;

public class SegmentEmptyProcedure {

    public String segmentID;
    public Map<String, Object> paramsMap;

    public SegmentEmptyProcedure(
            String segmentID, Map<String, Object> paramsMap) {
        super();
        this.segmentID = segmentID;
        this.paramsMap = paramsMap;
    }

    public List<AbstractTask> generateTasks() {
        
        List<AbstractTask> l = new ArrayList<AbstractTask>();
        
        //always check if segment exists
        CheckPathTask cpt = new CheckPathTask("spark-segment:" + segmentID, paramsMap);
        cpt.mustExist = true;
        cpt.mustnotExist = false;
        cpt.acceptFiles = true;
        cpt.acceptDirectories = true;
        cpt.validFileExtensions = new String[]{};
        cpt.singleDirectory = true;
        
        l.add(cpt);
            
        l.add(new SegmentEmptyTask(segmentID, paramsMap));
        
        return l;

    }
    
}
