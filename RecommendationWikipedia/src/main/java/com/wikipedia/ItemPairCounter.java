package com.wikipedia;

import com.datatorrent.api.Context;
import com.datatorrent.lib.algo.UniqueCounter;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

/**
 * Created by anurag on 27/6/16.
 */
public class ItemPairCounter extends UniqueCounter<String> implements LoggerFactory{

    private boolean change = false;
    int operatorId;

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        operatorId = context.getId();

    }

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
        change = false;
        makeNewLoggerInstance("inside a window");
    }

    @Override
    public void processTuple(String tuple) {
        super.processTuple(tuple);
        change = true;
        makeNewLoggerInstance("tuple processed"+ tuple +"is on operator:..."+operatorId);
    }

    @Override
    public void endWindow() {
        if(change) {
            super.endWindow();
            makeNewLoggerInstance("inside end window");
        }
        makeNewLoggerInstance("inside end window outside change");

    }

    @Override
    public Logger makeNewLoggerInstance(String s) {
        Logger log= Logger.getLogger(ItemPairCounter.class);
        log.info(s);
        return log;
    }
}
