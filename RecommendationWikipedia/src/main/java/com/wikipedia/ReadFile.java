package com.wikipedia;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by anurag on 20/6/16.
 */
public class ReadFile extends AbstractFileInputOperator<String> implements LoggerFactory{
    public transient final DefaultOutputPort<Entry> output = new DefaultOutputPort<>();
    public transient final DefaultOutputPort<Entry> vector = new DefaultOutputPort<>();
    private transient BufferedReader br;
    Boolean prefRead = false;
    Boolean toggle = false;
    Integer count,fileReadCount ;
    Vector userVector;
    Random rand= new Random(1);
    Entry map;

    @Override
    protected InputStream openFile(Path path) throws IOException {
        InputStream is= super.openFile(path);
        br = new BufferedReader(new InputStreamReader(is));
        return is;
    }

    @Override
    public void setup(Context.OperatorContext context) {

        super.setup(context);
        fileReadCount=1;
    }

    @Override
    public void beginWindow(long windowId) {
        count=0;
    }

    @Override
    public void endWindow() {
        super.endWindow();
        if(toggle) {
            prefRead = true;
            toggle = false;
        }
    }

    @Override
    protected String readEntity() throws IOException {
        String s = br.readLine();
        if(s == null) {
            toggle = true;
            fileReadCount++;
            if(fileReadCount<=2)
                processedFiles.clear();
        }
        return s;
    }
    @Override
    protected void emit(String s) {

        Pattern Numbers = Pattern.compile("(\\d+)");
        Matcher m = Numbers.matcher(s);

        m.find();
        Integer userId = Integer.valueOf(m.group());
        userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        while (m.find()) {
            int pref = rand.nextInt((10 - 1) + 1) + 1;
            userVector.set(Integer.parseInt(m.group()), 5);
        }
        map = new Entry(userId, userVector);
//        makeNewLoggerInstance("T "+toggle.toString()+" P "+prefRead.toString());
        if(!prefRead){
            vector.emit(map);
//            makeNewLoggerInstance(map.size()+" Vector ");
        }
        else {
            output.emit(map);
//            makeNewLoggerInstance(map.size()+" Output ");
        }


    }

    @Override
    protected void closeFile(InputStream is) throws IOException {
        super.closeFile(is);
        br.close();
    }

    @Override
    public Logger makeNewLoggerInstance(String s) {
        Logger log =Logger.getLogger(ReadFile.class);
        log.info(s);
        return log;
    }
}
