package com.wikipedia;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * Created by anurag on 28/6/16.
 */
public class WriteToFile extends AbstractFileOutputOperator<String>
{
  public WriteToFile()
  {
  }

  @Override
  protected String getFileName(String integerVectorHashMap)
  {
    return "Recommendation.txt";
  }

  @Override
  protected byte[] getBytesForTuple(String integerVectorHashMap)
  {
    return integerVectorHashMap.toString().getBytes();
  }

  @Override
  protected void processTuple(String tuple)
  {
    super.processTuple(tuple);
  }
}
