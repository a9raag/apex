package com.wikipedia;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by anurag on 20/6/16.
 */
public class ReadFile extends AbstractFileInputOperator<String>
{
  public transient final DefaultOutputPort<EntryPair> output = new DefaultOutputPort<>();
  public transient final DefaultOutputPort<EntryPair> vector = new DefaultOutputPort<>();
  private transient BufferedReader br;
  Boolean prefRead = false;
  Boolean toggle = false;
  int count, fileReadCount;
  Vector userVector;
  Random rand = new Random(1);
  EntryPair map;

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    fileReadCount = 1;
    emitBatchSize = 1;
  }

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (toggle) {
      prefRead = true;
      toggle = false;
    }
  }

  @Override
  protected String readEntity() throws IOException
  {
    String s = br.readLine();
    if (s == null) {
      toggle = true;
      fileReadCount++;
      if (fileReadCount <= 2)
        processedFiles.clear();
    }
    return s;
  }

  @Override
  protected void emit(String s)
  {

    Pattern Numbers = Pattern.compile("(\\d+)");
    Matcher m = Numbers.matcher(s);

    m.find();
    Integer userId = Integer.valueOf(m.group());
    userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
    while (m.find()) {
      userVector.set(Integer.parseInt(m.group()), ThreadLocalRandom.current().nextDouble(10));
    }
    map = new EntryPair(userId, userVector);
    if (!prefRead) {
      vector.emit(map);
    } else {
      output.emit(map);
    }
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    super.closeFile(is);
    br.close();
  }
}
