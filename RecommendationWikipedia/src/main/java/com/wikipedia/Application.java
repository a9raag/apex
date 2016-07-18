/**
 * Put your copyright and license info here.
 */
package com.wikipedia;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.RandomUtils;

@ApplicationAnnotation(name = "WikipediaApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomUtils.useTestSeed();
    ReadFile input = dag.addOperator("Input", new ReadFile());
    Cooccurrences pairs = dag.addOperator("Cooccurrences", new Cooccurrences());
    Counter counter = dag.addOperator("Counter", new Counter());
    counter.setCumulative(true);
    Recommender recommender = dag.addOperator("Recommender", new Recommender());
    WriteToFile fileWriter = dag.addOperator("Output", new WriteToFile());
    dag.addStream("Read The File", input.output, pairs.input);
    dag.addStream("UserVectors", input.vector, recommender.userVector);
    dag.addStream("Produce Cooccurrence", pairs.pairOutput, counter.data);
    dag.addStream("BuildR from Cooccurrences", counter.count, recommender.xyInput);
    dag.addStream("Update Recommendations", recommender.output, fileWriter.input);
  }
}
