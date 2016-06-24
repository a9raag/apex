package com.wikipedia;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by anurag on 20/6/16.
 */
public class ReadFile extends AbstractFileInputOperator<String> {
    public transient final DefaultOutputPort<HashMap<Integer,Vector>> output = new DefaultOutputPort<>();
    public transient final DefaultOutputPort<HashMap<Integer,Vector>> vector = new DefaultOutputPort<>();
    private transient BufferedReader br;
    int numTuples=100;
    int count=0;
    @Override
    protected InputStream openFile(Path path) throws IOException {
        InputStream is= super.openFile(path);
        br = new BufferedReader(new InputStreamReader(is));
        return is;
    }
    public void setNumTuples(int numTuples)
    {
        this.numTuples = numTuples;
    }

    @Override
    public void beginWindow(long windowId) {
        count=0;
    }

    @Override
    protected String readEntity() throws IOException {
        return br.readLine();
    }
    @Override
    protected void emit(String s) {

            Pattern Numbers = Pattern.compile("(\\d+)");
            Matcher m = Numbers.matcher(s);
            HashMap<Integer, Vector> map = new HashMap<>();
            m.find();
            Integer userId = Integer.valueOf(m.group());
            Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
            List<Integer> itemIDs = new ArrayList<>();
            while (m.find()) {
                int pref = (int) (Math.random() * 10);
                if (pref == 0)
                    pref += 1;
                userVector.set(Integer.parseInt(m.group()), pref);
            }
            map.put(userId, userVector);
            output.emit(map);
            vector.emit(map);


    }

    @Override
    protected void closeFile(InputStream is) throws IOException {
        super.closeFile(is);
        br.close();
    }
}
