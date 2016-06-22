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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by anurag on 20/6/16.
 */
public class ReadFile extends AbstractFileInputOperator<String> {
    public transient final DefaultOutputPort<HashMap<Integer,Vector>> output = new DefaultOutputPort<>();

    private transient BufferedReader br;

    @Override
    protected InputStream openFile(Path path) throws IOException {
        InputStream is= super.openFile(path);
        br = new BufferedReader(new InputStreamReader(is));
        return is;
    }



    @Override
    protected String readEntity() throws IOException {
        return br.readLine();
    }

    @Override
    protected void emit(String s) {
        Pattern Numbers= Pattern.compile("(\\d+)");
        Matcher m = Numbers.matcher(s);
        HashMap<Integer,Vector> map = new HashMap<>();
        m.find();
        Integer userId= Integer.valueOf(m.group());
        Vector userVector= new RandomAccessSparseVector(Integer.MAX_VALUE,100);
        List<Integer> itemIDs= new ArrayList<>();
        while (m.find()) {
            userVector.set(Integer.parseInt(m.group()),1.0f);
        }
        map.put(userId,userVector);
        output.emit(map);

    }

}
