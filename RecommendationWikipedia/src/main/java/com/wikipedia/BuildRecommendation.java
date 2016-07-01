package com.wikipedia;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;


public class BuildRecommendation extends BaseOperator implements LoggerFactory{
    public transient  final DefaultOutputPort<String> Rout = new DefaultOutputPort<>();

//    @FieldSerializer.Bind(JavaSerializer.class)
    public transient Matrix userMatrix,Rmatrix,cMatrix;
    Integer numUsers;
    public transient final DefaultInputPort<Entry> userVector = new DefaultInputPort<Entry>() {
        @Override
        public void process(Entry tuple) {
            Integer key=tuple.getUid();
            userMatrix.assignRow(key,tuple.getV());
            numUsers++;
        }
    };

    @Override
    public void setup(Context.OperatorContext context) {
        numUsers=new Integer(0);
        userMatrix=new SparseMatrix(Integer.MAX_VALUE,Integer.MAX_VALUE);
        Rmatrix  = new SparseMatrix(Integer.MAX_VALUE,Integer.MAX_VALUE);
//        cMatrix= new SparseMatrix(Integer.MAX_VALUE,Integer.MAX_VALUE);
    }

    public transient final DefaultInputPort<String> xyInput=new DefaultInputPort<String>() {
        @Override
        public void process(String tuple) {
            Vector R;
            String [] xy=tuple.split(":");
            Integer X= Integer.valueOf(xy[0]);
            Integer Y= Integer.valueOf(xy[1]);
            Double count =1.0;
//            cMatrix.set(X,Y,count);

            for(int i=1;i<=numUsers;i++){
                Vector U=userMatrix.viewRow(i);
                R=Rmatrix.viewRow(i);
                double Rx=R.get(X);
                R.set(X,Rx+U.get(Y)*count);
                Rmatrix.assignRow(i,R);
            }
            for(Integer i=1;i<=numUsers;i++){
                Rout.emit(i.toString()+"\t"+Rmatrix.viewRow(i)+"\n");
            }

        }
    };

    @Override
    public void endWindow() {
        super.endWindow();
    }

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
    }

    @Override
    public Logger makeNewLoggerInstance(String s) {
        Logger log = Logger.getLogger(BuildRecommendationTest.class);
        log.info(s);
        return  log;
    }
}
