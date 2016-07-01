package com.wikipedia;

import org.apache.mahout.cf.taste.impl.recommender.ByValueRecommendedItemComparator;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.*;
import org.apache.mahout.math.Vector;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by anurag on 19/6/16.
 */
public class Test {
    static final Pattern NUMBERS = Pattern.compile("(\\d+:\\d+)");

    public static void main(String args[]){
        String user="8: 5 57544 58089 59375 64985 313376 704624 717529 729993 1204280 1204407 1254637 1255317 1497566 1720928 1850305 2269887 2333350 2359764 2496900 2640848 2743982 3303009 3322952 3492254 3573013 3797343 3797349 3797359 4033556 4173124 4189168 4206743 4207986 4393611 4813259 4901416 5010479 5062062 5072938 5098953 5292042 5429924 5599862 5599863";
        user =" 723={1512789:33.0,104440:560.0,3685696:45.0,707592:1.0,4088623:500.0,2182675:1.0,421957:4.0,1066954:10.0,1451913:20.0,446508:580.0,760561:61.0,681886:7.0,3334949:4.0,4572589:5.0,978616:7.0,1720889:12.0,2307050:1020.0,2435485:6.0}";

        Matcher m =NUMBERS.matcher(user);
        Vector recommendationVector= new RandomAccessSparseVector(Integer.MAX_VALUE,100);
//        System.out.println(recommendationVector);
        int n = 65537;
        Double d= new Double(n * (n + 1) / 2);
        System.out.println(d);
        Matrix symmetricMatrix= new DenseSymmetricMatrix(n);

        symmetricMatrix.set(1,3,2.0);
        symmetricMatrix.get(112,312);
//        System.out.println(upperTriangular.get(3,1));
//        System.out.println(upperTriangular.viewRow(1));
//        System.out.println(upperTriangular);
        while(m.find()){
            String [] v= m.group().split(":");
            recommendationVector.set(Integer.parseInt(v[0]),Double.parseDouble(v[1]));
        }

        Integer recommendationsPerUser=10;
        HashMap<Integer,RandomAccessSparseVector> map= new HashMap<>();
        Queue<RecommendedItem> topItems =new PriorityQueue<>(recommendationsPerUser,Collections.<RecommendedItem>reverseOrder(ByValueRecommendedItemComparator.getInstance()));
        Iterator<Vector.Element> recommendationVectorIterator =
                recommendationVector.nonZeroes().iterator();
        while (recommendationVectorIterator.hasNext()) {
            Vector.Element element = recommendationVectorIterator.next();
            int index = element.index();
            float value = (float) element.get();
            if (topItems.size() < recommendationsPerUser) {
                topItems.add(new GenericRecommendedItem(
                        index, value));
            } else if (value > topItems.peek().getValue()) {
                topItems.add(new GenericRecommendedItem(
                        index, value));
                topItems.poll();
            }
        }
        map.put(723, (RandomAccessSparseVector) recommendationVector);
        Matrix matrix =new SparseMatrix(Integer.MAX_VALUE,Integer.MAX_VALUE);
        matrix.assignRow(723,recommendationVector);
//        System.out.println(matrix.get(723,104440));
        List<RecommendedItem> recommendations =
                new ArrayList<RecommendedItem>(topItems.size());
        recommendations.addAll(topItems);
        Collections.sort(recommendations,
                ByValueRecommendedItemComparator.getInstance());
//        System.out.println(recommendations);
        Random rand= new Random(1);

        for(int i=0;i<100;i++){
            int randomNum = rand.nextInt((10 - 1) + 1) + 1;
            System.out.print(randomNum+"\t");
        }

    }
}
