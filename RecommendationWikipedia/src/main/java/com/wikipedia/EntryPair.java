package com.wikipedia;

import java.util.Map;

/**
 * Created by anurag on 30/6/16.
 */
public class EntryPair
{

  int uid;
  Map<Integer, Double> v;

  public EntryPair()
  {
  }

  public EntryPair(int uid, Map<Integer, Double> v)
  {
    this.uid = uid;
    this.v = v;
  }

  public int getUid()
  {
    return uid;
  }

  public void setUid(int uid)
  {
    this.uid = uid;
  }

  public Map<Integer, Double> getV()
  {
    return v;
  }

  public void setV(Map<Integer, Double> v)
  {
    this.v = v;
  }
}
