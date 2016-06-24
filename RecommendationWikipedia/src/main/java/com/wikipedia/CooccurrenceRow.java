package com.wikipedia;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by anurag on 21/6/16.
 */
public class CooccurrenceRow extends BaseOperator  {
    public  transient  final DefaultOutputPort<String> coOccures= new DefaultOutputPort<>();
    public transient final DefaultInputPort<HashMap<Integer,Vector>> hashInput=new DefaultInputPort<HashMap<Integer, Vector>>() {

        @Override
        public void process(HashMap<Integer, Vector> tuple) {
            int key=tuple.keySet().iterator().next();
            Iterable<Vector.Element> iterable = tuple.get(key).nonZeroes();
            Iterator<Vector.Element> it= iterable.iterator();
            log.debug(" Sending: {}" , it.toString());
            while (it.hasNext()) {
                Integer index1=it.next().index();
                Iterable<Vector.Element> iterable1= tuple.get(key).nonZeroes();
                Iterator<Vector.Element> it2=iterable1.iterator();
                while(it2.hasNext()){
                    Integer index2=it2.next().index();
                    coOccures.emit(index1.toString()+":"+index2.toString());
                    }
               }
            }


    };

    Logger log  = LoggerFactory.getLogger(CooccurrenceRow.class);

}
