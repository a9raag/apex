package com.wikipedia;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import org.apache.mahout.math.Vector;

import java.util.HashMap;

/**
 * Created by anurag on 24/6/16.
 */
public class RecommendationList extends AbstractFileOutputOperator<String> {
    public transient  final DefaultInputPort <HashMap<Integer,Vector>> recMap = new DefaultInputPort<HashMap<Integer, Vector>>() {
        @Override
        public void process(HashMap<Integer, Vector> tuple) {

        }
    };

    public RecommendationList(){
        filePath="Recommendations";
    }
    @Override
    protected String getFileName(String s) {
        return "Recommendations";
    }

    @Override
    protected byte[] getBytesForTuple(String s) {
        return s.getBytes();
    }
}
