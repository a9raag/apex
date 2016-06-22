package com.wikipedia;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

/**
 * Created by anurag on 22/6/16.
 */
public class CooccureneceRows extends BaseOperator{
    public transient final DefaultInputPort<String> cooccurreneces=new DefaultInputPort<String>() {
        @Override
        public void process(String tuple) {
           Vector CooccureneceRow = new RandomAccessSparseVector(Integer.MAX_VALUE,100);

        }
    };
}
