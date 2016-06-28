package com.wikipedia;

import com.datatorrent.lib.algo.UniqueCounter;

/**
 * Created by anurag on 27/6/16.
 */
public class RecommendationStart extends UniqueCounter<String> {

    private boolean change = false;

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
        change = false;
    }

    @Override
    public void processTuple(String tuple) {
        super.processTuple(tuple);
        change = true;
    }

    @Override
    public void endWindow() {
        if(change) {
            super.endWindow();
        }
    }
}
