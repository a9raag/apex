package com.anurag;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * Created by anurag on 17/6/16.
 */
public class WriteToFile extends AbstractFileOutputOperator<String> {
    public WriteToFile(){
        filePath = "NumClass";
    }
    @Override
    protected String getFileName(String s) {
        return "Even.txt";
    }

    @Override
    protected byte[] getBytesForTuple(String s) {
        return  s.getBytes();
    }
}
