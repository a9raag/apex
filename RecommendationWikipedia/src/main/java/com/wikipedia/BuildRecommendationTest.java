package com.wikipedia;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.mahout.math.*;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by anurag on 30/6/16.
 */
public class BuildRecommendationTest extends BaseOperator implements LoggerFactory{
    Vector R;
    Matrix userMatrix,Rmatrix;
    HashMap<Integer,RandomAccessSparseVector> userMaps;
    Integer numUsers;
    public transient  final DefaultOutputPort<String> Rout = new DefaultOutputPort<>();
    @Override
    public void setup(Context.OperatorContext context) {

        userMaps =new HashMap<>();
        numUsers=new Integer(0);
        userMatrix=new SparseMatrix(Integer.MAX_VALUE,Integer.MAX_VALUE);
        Rmatrix  = new SparseMatrix(Integer.MAX_VALUE,Integer.MAX_VALUE);
        super.setup(context);
    }

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
    }

    public transient final DefaultInputPort<HashMap<Integer,Vector>> userVector = new DefaultInputPort<HashMap<Integer, Vector>>() {
        @Override
        public void process(HashMap<Integer, Vector> tuple) {
            Integer key=tuple.keySet().iterator().next();
            userMatrix.assignRow(key,tuple.get(key));
            numUsers++;
        }
    };
    public transient final DefaultInputPort<HashMap<String,Integer>> xyInput=new DefaultInputPort<HashMap<String, Integer>>() {
        @Override
        public void process(HashMap<String, Integer> tuple) {

            for(int i=1;i<=numUsers;i++){
                Vector v=userMatrix.viewRow(i);
                Iterator<String> keys=tuple.keySet().iterator();
                R= Rmatrix.viewRow(i);
                while(keys.hasNext()){
                    String key=keys.next();
                    String [] xy=key.split(":");
                    Integer coCount=tuple.get(key);
                    Integer X= Integer.valueOf(xy[0]);
                    Integer Y= Integer.valueOf(xy[1]);
                    R.set(X,R.get(X)+v.get(Y)*coCount);

                    Rmatrix.assignRow(i,R);


                }
                if(R!=null) {
                    Rmatrix.assignRow(i, R);
                    makeNewLoggerInstance("User Id:" + i + " R\t" + Rmatrix.viewRow(i));
                    Rout.emit("User Id:" + i + " R\t" + Rmatrix.viewRow(i) + "\n");
                }

            }


        }
    };
    @Override
    public Logger makeNewLoggerInstance(String s) {
        Logger log = Logger.getLogger(BuildRecommendationTest.class);
        log.info(s);
        return  log;
    }
}
