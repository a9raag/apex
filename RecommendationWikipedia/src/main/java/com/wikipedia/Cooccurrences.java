package com.wikipedia;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.mahout.math.Vector;

import java.util.Iterator;

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
      Iterable<Vector.Element> iterable = tuple.getV().nonZeroes();
      Iterator<Vector.Element> it = iterable.iterator();
      while (it.hasNext()) {
        Integer index1 = it.next().index();
        Iterable<Vector.Element> iterable1 = tuple.getV().nonZeroes();
        Iterator<Vector.Element> it2 = iterable1.iterator();
        while (it2.hasNext()) {
          Integer index2 = it2.next().index();
          pairOutput.emit(index1 + ":" + index2);
        }
      }
    }
  };
}
