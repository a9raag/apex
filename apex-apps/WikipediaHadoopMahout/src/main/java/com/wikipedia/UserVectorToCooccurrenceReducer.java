package com.wikipedia;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by anurag on 14/6/16.
 */
public class UserVectorToCooccurrenceReducer extends Reducer<IntWritable,IntWritable,IntWritable,VectorWritable> {
    @Override
    protected void reduce(IntWritable itemIndex1, Iterable<IntWritable> itemIndex2s, Context context) throws IOException, InterruptedException {
        Vector cooccureneceRow= new RandomAccessSparseVector(Integer.MAX_VALUE,100);
        for(IntWritable intWritable :itemIndex2s){
            int itemIndex2= intWritable.get();
            cooccureneceRow.set(itemIndex2,cooccureneceRow.get(itemIndex2)+1.0);
        }
        context.write(itemIndex1,new VectorWritable(cooccureneceRow));
    }
}
