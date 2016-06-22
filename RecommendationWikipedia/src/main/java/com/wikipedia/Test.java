package com.wikipedia;

import org.apache.mahout.math.*;
import org.apache.mahout.math.Vector;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by anurag on 19/6/16.
 */
public class Test {
    static final Pattern NUMBERS = Pattern.compile("(\\d+)");
    static final String user="8: 5 57544 58089 59375 64985 313376 704624 717529 729993 1204280 1204407 1254637 1255317 1497566 1720928 1850305 2269887 2333350 2359764 2496900 2640848 2743982 3303009 3322952 3492254 3573013 3797343 3797349 3797359 4033556 4173124 4189168 4206743 4207986 4393611 4813259 4901416 5010479 5062062 5072938 5098953 5292042 5429924 5599862 5599863";
    public static void main(String args[]){
        Map<String,List<Integer>> numbers=new HashMap<>();
        List<Integer> nums= new ArrayList<>();
        Matcher m =NUMBERS.matcher(user);

        m.find();


        Vector userVector= new RandomAccessSparseVector(Integer.MAX_VALUE,100);
        VectorWritable v = new VectorWritable();
        List<Integer> itemIDs= new ArrayList<>();
        while (m.find()) {
            userVector.set(Integer.parseInt(m.group()),1.0f);
        }
        HashMap<Integer,Vector> map= new HashMap<>();
        map.put(8,userVector);
        int key=map.keySet().iterator().next();
        Iterable<Vector.Element> iterable= userVector.nonZeroes();
        Iterator<Vector.Element> it=iterable.iterator();
        while(it.hasNext()){
            int index1=it.next().index();
            System.out.println(index1);

        }
        System.out.println(key);
        System.out.println(map.get(key));
    }
}
