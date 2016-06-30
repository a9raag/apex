package com.wikipedia;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.NumberAggregate;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.InteractionValueEncoder;

import java.sql.Array;
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

//    public transient  final DefaultOutputPort<Boolean> delayOutput=new DefaultOutputPort<>();
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
           // makeNewLoggerInstance("xyIput: " + tuple);
           // getCoCount(Yindex,tuple);
            makeNewLoggerInstance("usermaps " + userMaps);
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
                Integer co_count = tuple.get(key);

               // makeNewLoggerInstance("co_count X Y " + co_count +" "+X +" "+Y);
               //Iterator<HashMap<Integer,Vector>> userIterator=userMaps.iterator();
            /*    while(userIterator.hasNext()) {
                    HashMap<Integer,Vector> userMap=userIterator.next();
                    //makeNewLoggerInstance("single user map" + userMap);
                    Iterator<Integer> userIds=userMap.keySet().iterator();
                    while(userIds.hasNext()) {
                        Integer userID = userIds.next();
                        Vector u = userMap.get(userID);
                        Double rIndex = R.get(X);
                        Double uIndex = u.get(Y);
                        makeNewLoggerInstance("uIndex" + uIndex);
                       R[x] += U[y]*Cooccurrences
                        R.set(X, rIndex + uIndex * co_count);
                        output.put(userID, R);
                    }
                }*/
            }

            Iterator<HashMap<Integer,Vector>> userIterator=userMaps.iterator();
            while(userIterator.hasNext()) {
                HashMap<Integer, Vector> userMap = userIterator.next();
                makeNewLoggerInstance("single user map" + userMap);
                Iterator<Integer> userIds = userMap.keySet().iterator();
                while(userIds.hasNext()){
                    R= new RandomAccessSparseVector(Integer.MAX_VALUE,100);
                    Integer uid=userIds.next();
                    Vector itemPref=userMap.get(uid);
                    makeNewLoggerInstance("userd ids " + uid);
                    Double finalans=0.0;
                    for(int i=1;i<=4;i++){
                        Double answer=0.0;
                        ArrayList<String> b = getCoCount(i,tuple);
                        for (int j=0;j<b.size();j++) {
                            String x[] = b.get(j).split(" ");
                            makeNewLoggerInstance("split : C" + x[0] + " X" + x[1] + " Y" + x[2]);
                            double pref = itemPref.get(Integer.parseInt(x[1]));
                            answer += pref * Double.parseDouble(x[0]);
                        }
                       makeNewLoggerInstance("Answer :"+uid + "::" + i+" "+answer);
                        R.set(i,answer);
                        output.put(uid,R);
                    }
                    Rout.emit(output.toString());
                }
            }

        }


    };

    public ArrayList<String> getCoCount(int Yindex, HashMap<String,Integer> tuple) {

        ArrayList<String> a = new ArrayList<>();
        Iterator<String> keyIterator = tuple.keySet().iterator();
       // makeNewLoggerInstance("tuple :" +tuple);
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            Pattern pattern = Pattern.compile("(\\d+)");
            Matcher m = pattern.matcher(key);
            m.find();
            Integer X = Integer.parseInt(m.group());
            m.find();
            Integer Y = Integer.parseInt(m.group());
            Integer co_count = tuple.get(key);
            //makeNewLoggerInstance("Y ::" +Y+ " X ::"+X);
            if (Y.intValue() == Yindex)
                a.add(""+co_count+" "+X +" "+Y);

        }
        makeNewLoggerInstance("for "+Yindex+"= :" +a);
        return a;
    }
    @Override
    public Logger makeNewLoggerInstance(String s) {
        Logger log =Logger.getLogger(BuildRecommendation.class);
        log.info(s);
        return  log;
    }
}
