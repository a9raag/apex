package com.wikipedia;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.mahout.math.Vector;


import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by anurag on 21/6/16.
 */
public class CreateItemPair extends BaseOperator implements LoggerFactory {
    public  transient  final DefaultOutputPort<String> coOccures= new DefaultOutputPort<>();
    public transient final DefaultInputPort<HashMap<Integer,Vector>> hashInput=new DefaultInputPort<HashMap<Integer, Vector>>() {

        @Override
        public void process(HashMap<Integer, Vector> tuple) {
            int key=tuple.keySet().iterator().next();
            Iterable<Vector.Element> iterable = tuple.get(key).nonZeroes();
            Iterator<Vector.Element> it= iterable.iterator();

            while (it.hasNext()) {
                Integer index1=it.next().index();
                Iterable<Vector.Element> iterable1= tuple.get(key).nonZeroes();
                Iterator<Vector.Element> it2=iterable1.iterator();
                while(it2.hasNext()){
                    Integer index2=it2.next().index();
                    coOccures.emit(index1.toString()+":"+index2.toString());
//                    makeNewLoggerInstance("CreateItemPair {}"+index1.toString());
                    }
               }
            }


    };


    @Override
    public Logger makeNewLoggerInstance(String s) {
        Logger log = Logger.getLogger(CreateItemPair.class);
        log.info(s);
        return log;
    }
}
