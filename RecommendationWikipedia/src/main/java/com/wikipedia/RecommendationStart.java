package com.wikipedia;

import com.datatorrent.lib.algo.UniqueCounter;

/**
 * Created by anurag on 27/6/16.
 */
public class RecommendationStart extends UniqueCounter<String> {
    @Override
    public void endWindow() {
        super.endWindow();
    }
}
