/**
 * Put your copyright and license info here.
 */
package com.wikipedia;

import com.datatorrent.lib.algo.UniqueCounter;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="WikipediaApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

//    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
//    randomGenerator.setNumTuples(500);
//
      ReadFile readFile = dag.addOperator("readFile",new ReadFile());
      readFile.setNumTuples(1000);
      CooccurrenceRow cRow=dag.addOperator("cooccurrenceRow",new CooccurrenceRow());
      UniqueCounter<String> counter= dag.addOperator("Cooccurrences",new UniqueCounter<String>());
      ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());
      BuildRecommendation buildR= dag.addOperator("buildR",new BuildRecommendation());
      dag.addStream("Read The File",readFile.output,cRow.hashInput);
      dag.addStream("UserVectos",readFile.vector,buildR.userVector);
      dag.addStream("Produce Cooccurrence",cRow.coOccures,counter.data);
      dag.addStream("BuildR from Cooccurrences",counter.count,buildR.xyInput);
      dag.addStream("Proudce Cooccurrence Counter",buildR.Rout,cons.input);

//    dag.addStream("randomData", randomGenerator.out, cons.input).setLocality(Locality.CONTAINER_LOCAL);
  }
}
