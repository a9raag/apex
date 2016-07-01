package com.wikipedia;

import org.apache.mahout.math.Vector;

/**
 * Created by anurag on 30/6/16.
 */
public class Entry {

    public Entry(){

    }

    public Entry(int uid, Vector v) {
        this.uid = uid;
        this.v = v;
    }
    int uid;
    Vector v;

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public Vector getV() {
        return v;
    }

    public void setV(Vector v) {
        this.v = v;
    }
}
