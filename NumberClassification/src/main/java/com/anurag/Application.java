/**
 * Put your copyright and license info here.
 */
package com.anurag;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Add Operators
    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
    randomGenerator.setNumTuples(500);
    NumberClassification numCl=dag.addOperator("NumberClassifier",NumberClassification.class);
//    ConsoleOutputOperator consOdd = dag.addOperator("consoleOdd", new ConsoleOutputOperator());
//    ConsoleOutputOperator consPrime = dag.addOperator("consolePrime", new ConsoleOutputOperator());
    WriteToFile fileEven = dag.addOperator("fileEven",WriteToFile.class);
    WriteToFile filePrime = dag.addOperator("filePrime",WriteToFile.class);
    //Add Streams

    dag.addStream("Generator To Classifier",randomGenerator.out,numCl.input);
//    dag.addStream("Odd Number to Console",numCl.outputOdd,consOdd.input);
    dag.addStream("Even Number to File",numCl.outputEven,fileEven.input);
    dag.addStream("Prime Number to File",numCl.outputPrime,filePrime.input);

  }
}
