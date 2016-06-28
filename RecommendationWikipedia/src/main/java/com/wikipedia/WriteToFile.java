package com.wikipedia;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import org.apache.mahout.math.Vector;

import java.util.HashMap;

/**
 * Created by anurag on 28/6/16.
 */
public class WriteToFile extends AbstractFileOutputOperator<HashMap<Integer,Vector>> {
    public WriteToFile(){
        filePath="output/";
    }
    @Override
    protected String getFileName(HashMap<Integer, Vector> integerVectorHashMap) {
        return "Recommendation.txt";
    }

    @Override
    protected byte[] getBytesForTuple(HashMap<Integer, Vector> integerVectorHashMap) {
        return integerVectorHashMap.toString().getBytes();
    }
}
