package com.wikipedia;

import com.datatorrent.lib.algo.UniqueCounter;

/**
 * Created by anurag on 28/6/16.
 */
public class MyCounter extends UniqueCounter<String> {

    @Override
    public void processTuple(String tuple) {
        super.processTuple(tuple);
//        System.out.println("Processing: " + map.toString());
    }

    @Override
    public void endWindow() {
        System.out.println("Processing: " + map.size());
        super.endWindow();
    }
}
