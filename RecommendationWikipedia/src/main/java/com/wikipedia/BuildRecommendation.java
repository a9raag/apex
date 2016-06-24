package com.wikipedia;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.hadoop.util.hash.Hash;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by anurag on 23/6/16.
 */
public class BuildRecommendation extends BaseOperator {
    Vector R;
    List<HashMap<Integer,Vector>> userMaps;
    Integer cnt;
    public transient  final DefaultOutputPort<HashMap<Integer,Vector>> Rout = new DefaultOutputPort<>();
    @Override
    public void setup(Context.OperatorContext context) {
        R= new RandomAccessSparseVector(Integer.MAX_VALUE,100);
        userMaps =new ArrayList<>();
        super.setup(context);
    }



    public transient final DefaultInputPort<HashMap<Integer,Vector>> userVector = new DefaultInputPort<HashMap<Integer, Vector>>() {
        @Override
        public void process(HashMap<Integer, Vector> tuple) {
            userMaps.add(tuple);
        }
    };
    public transient final DefaultInputPort<HashMap<String,Integer>> xyInput= new DefaultInputPort<HashMap<String, Integer>>() {
        @Override
        public void process(HashMap<String,Integer> tuple) {
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
            Rout.emit(output);


        }


    };

}
