package com.anurag;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by anurag on 17/6/16.
 */
public class NumberClassification extends BaseOperator {
    public final transient  DefaultOutputPort<String> outputEven= new DefaultOutputPort<>();
    public final transient DefaultOutputPort<String> outputOdd= new DefaultOutputPort<>();
    public final transient DefaultOutputPort<String> outputPrime= new DefaultOutputPort<>();
    public transient  final DefaultInputPort<Integer> input= new DefaultInputPort<Integer>() {
        @Override
        public void process(Integer tuple) {


            if (tuple % 2 == 0)
                outputEven.emit("Even" + tuple.toString());
            else if (tuple % 2 != 0) {
                for (int i = 2; i <= tuple; i++) {
                    if (i == tuple)
                        outputPrime.emit("Prime" + tuple.toString());
                    else if (tuple % i == 0) {
                        outputOdd.emit("Odd" + tuple.toString());
                        break;
                    }
                }

            }
        }
    };
}
