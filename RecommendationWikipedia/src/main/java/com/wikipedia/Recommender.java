package com.wikipedia;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import java.util.HashMap;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;

public class Recommender extends BaseOperator
{
  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<>();

  public transient Matrix userMatrix, Rmatrix, cMatrix;
  Integer numUsers;
  public transient final DefaultInputPort<EntryPair> userVector = new DefaultInputPort<EntryPair>()
  {
    @Override
    public void process(EntryPair tuple)
    {
      Integer key = tuple.getUid();
      userMatrix.assignRow(key, tuple.getV());
      numUsers++;
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    numUsers = new Integer(0);
    userMatrix = new SparseMatrix(Integer.MAX_VALUE, Integer.MAX_VALUE);
    Rmatrix = new SparseMatrix(Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

  public transient final DefaultInputPort<HashMap<String, Integer>> xyInput = new DefaultInputPort<HashMap<String, Integer>>()
  {
    @Override
    public void process(HashMap<String, Integer> tuple)
    {
      Vector R;
      for (String userKey : tuple.keySet()) {
        String[] XY = userKey.split(":");
        int X = Integer.parseInt(XY[0]);
        int Y = Integer.parseInt(XY[1]);
        int count = tuple.get(userKey);

        for (int i = 1; i <= numUsers; i++) {
          Vector U = userMatrix.viewRow(i);
          R = Rmatrix.viewRow(i);
          double Rx = R.get(X);
          R.set(X, Rx + U.get(Y) * count);
          Rmatrix.assignRow(i, R);
        }
        for (Integer i = 1; i <= numUsers; i++) {
          output.emit(i.toString() + "\t" + Rmatrix.viewRow(i) + "\n");
        }
      }
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
  }
}
