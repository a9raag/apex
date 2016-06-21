package com.wikipedia;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by anurag on 15/6/16.
 */
public class WikipediaToUserVectorReducer extends Reducer<VarLongWritable,VarLongWritable,VarLongWritable,VectorWritable> {
    @Override
    protected void reduce(VarLongWritable userID, Iterable<VarLongWritable> itemPrefs, Context context) throws IOException, InterruptedException {
        Vector  userVector = new RandomAccessSparseVector(Integer.MAX_VALUE,100);
        for (VarLongWritable itemPref:itemPrefs){
            userVector.set((int)itemPref.get(),1.0f);
        }
        context.write(userID,new VectorWritable(userVector));

    }
}
