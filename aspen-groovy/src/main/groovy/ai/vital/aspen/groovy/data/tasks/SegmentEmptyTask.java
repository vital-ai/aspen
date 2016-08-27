package ai.vital.aspen.groovy.data.tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class SegmentEmptyTask extends AbstractTask {

    public String segmentID = null;
    
    public SegmentEmptyTask(String segmentID, Map<String, Object> globalParameters) {
        super(globalParameters);
        this.segmentID = segmentID;
    }


    @Override
    public List<String> getRequiredParams() {
        return new ArrayList<String>();
    }

    @Override
    public List<String> getOutputParams() {
        return new ArrayList<String>();
    }

}
