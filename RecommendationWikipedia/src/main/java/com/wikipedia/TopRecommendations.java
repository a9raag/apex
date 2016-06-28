package com.wikipedia;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by anurag on 28/6/16.
 */
public class TopRecommendations extends BaseOperator {
    static final Pattern NUMBERS = Pattern.compile("(\\d+:\\d+)");
    Matcher m ;
    DefaultInputPort<String> recommendationRow=new DefaultInputPort<String>() {
        @Override
        public void process(String tuple) {
           m =NUMBERS.matcher(tuple);
        }
    };
}
