package com.wikipedia;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Maps;

import java.util.HashMap;

public class Recommender extends BaseOperator
{
  public transient final DefaultOutputPort<String> output = new DefaultOutputPort<>();

  HashMap<Integer, HashMap<Integer, Double>> userPreferences;
  HashMap<Integer, HashMap<Integer, Double>> recommendations;
  int numUsers;

  public transient final DefaultInputPort<EntryPair> userVector = new DefaultInputPort<EntryPair>()
  {
    @Override
    public void process(EntryPair tuple)
    {
      int uid = tuple.getUid();
      HashMap<Integer, Double> v = (HashMap<Integer, Double>)tuple.getV();
      userPreferences.put(uid, v);
      numUsers++;
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    numUsers = 0;
    userPreferences = Maps.newHashMap();
    recommendations = Maps.newHashMap();
  }

  public transient final DefaultInputPort<HashMap<String, Integer>> xyInput = new DefaultInputPort<HashMap<String, Integer>>()
  {
    @Override
    public void process(HashMap<String, Integer> tuple)
    {
      HashMap<Integer, Double> recommendation;
      double Rx = 0;
      for (String userKey : tuple.keySet()) {
        String[] XY = userKey.split(":");
        int X = Integer.parseInt(XY[0]);
        int Y = Integer.parseInt(XY[1]);
        int count = tuple.get(userKey);

        for (int i: userPreferences.keySet()) {
          HashMap<Integer, Double> prefs = userPreferences.get(i);
          if(! prefs.containsKey(Y)) {
            continue;
          }
          recommendation = recommendations.get(i);
          if(recommendation != null && recommendation.containsKey(X)) {
            Rx = recommendation.get(X);
          } else {
            recommendation = new HashMap<>();
          }
          recommendation.put(X, Rx + prefs.get(Y) * count);
          recommendations.put(i, recommendation);
          output.emit(i + "\t" + recommendations.get(i) + "\n");
        }
      }
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
  }
}
