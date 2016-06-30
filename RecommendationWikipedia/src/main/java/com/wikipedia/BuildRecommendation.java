package com.wikipedia;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class BuildRecommendation extends BaseOperator implements LoggerFactory {
    Vector R;
    List<HashMap<Integer,Vector>> userMaps;
    Integer cnt;

    public transient  final DefaultOutputPort<String> Rout = new DefaultOutputPort<>();
    @Override
    public void setup(Context.OperatorContext context) {
        R= new RandomAccessSparseVector(Integer.MAX_VALUE,100);
        userMaps =new ArrayList<>();
        cnt=new Integer(0);
        super.setup(context);
    }

    public transient final DefaultInputPort<HashMap<Integer,Vector>> userVector = new DefaultInputPort<HashMap<Integer, Vector>>() {
        @Override
        public void process(HashMap<Integer, Vector> tuple) {
            userMaps.add(tuple);
//            makeNewLoggerInstance("userVector");

        }
    };
    public transient final DefaultInputPort<HashMap<String,Integer>> xyInput= new DefaultInputPort<HashMap<String, Integer>>() {
        @Override
        public void process(HashMap<String,Integer> tuple) {
            makeNewLoggerInstance("xyIput: " + tuple);
            HashMap<Integer,Vector> output=new HashMap<>();
            Iterator<String> keyIterator=tuple.keySet().iterator();
            while(keyIterator.hasNext()) {
                String key=keyIterator.next();
                Pattern pattern = Pattern.compile("(\\d+)");
                Matcher m = pattern.matcher(key);
                m.find();
                Integer X = Integer.parseInt(m.group());
                m.find();
                Integer Y = Integer.parseInt(m.group());
                Integer pref = tuple.get(key);
                Iterator<HashMap<Integer,Vector>> userIterator=userMaps.listIterator();
                while(userIterator.hasNext()) {
                    HashMap<Integer,Vector> userMap=userIterator.next();
                    Iterator<Integer> userIds=userMap.keySet().iterator();
                    while(userIds.hasNext()) {
                        Integer userID = userIds.next();
                        Vector u = userMap.get(userID);
                        Double rIndex = R.get(X);
                        Double uIndex = u.get(Y);
//                        R[x] += U[y]*Cooccurrences
                        R.set(X, rIndex + uIndex * pref);
                        output.put(userID, R);
                    }


                }

            }
            Rout.emit(output.toString());
        }


    };

    @Override
    public Logger makeNewLoggerInstance(String s) {
        Logger log =Logger.getLogger(BuildRecommendation.class);
        log.info(s);
        return  log;
    }
}
