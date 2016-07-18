package com.wikipedia;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import java.util.Map.Entry;

/**
 * Created by anurag on 21/6/16.
 */
public class Cooccurrences extends BaseOperator
{
  public transient final DefaultOutputPort<String> pairOutput = new DefaultOutputPort<>();
  public transient final DefaultInputPort<EntryPair> input = new DefaultInputPort<EntryPair>()
  {

    @Override
    public void process(EntryPair tuple)
    {
      for (Entry<Integer, Double> entry1: tuple.getV().entrySet()) {
        for (Entry<Integer, Double> entry2: tuple.getV().entrySet()) {
          if(entry1.getKey() < entry2.getKey()) {
            pairOutput.emit(entry1.getKey() + ":" + entry2.getKey());
          } else {
            pairOutput.emit(entry2.getKey() + ":" + entry1.getKey());
          }
        }
      }
    }
  };
}
