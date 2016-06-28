/**
 * Put your copyright and license info here.
 */
package com.wikipedia;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import org.apache.hadoop.conf.Configuration;
@ApplicationAnnotation(name="WikipediaApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

//    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
//    randomGenerator.setNumTuples(500);
//

      ReadFile readFile = dag.addOperator("readFile",new ReadFile());
      readFile.setEmitBatchSize(10);
      CooccurrenceRow cRow=dag.addOperator("cooccurrenceRow",new CooccurrenceRow());
      MyCounter counter= dag.addOperator("Cooccurrences",new MyCounter());
      counter.setCumulative(true);
      WriteToFile cons = dag.addOperator("console", new WriteToFile());
      BuildRecommendation buildR= dag.addOperator("buildR",new BuildRecommendation());
      dag.addStream("Read The File",readFile.output,cRow.hashInput).setLocality(DAG.Locality.CONTAINER_LOCAL);
      dag.addStream("UserVectos",readFile.vector,buildR.userVector);
      dag.addStream("Produce Cooccurrence",cRow.coOccures,counter.data).setLocality(DAG.Locality.CONTAINER_LOCAL);
      dag.addStream("BuildR from Cooccurrences",counter.count,buildR.xyInput);
      dag.addStream("Proudce Cooccurrence Counter",buildR.Rout,cons.input).setLocality(DAG.Locality.CONTAINER_LOCAL);

//    dag.addStream("randomData", randomGenerator.out, cons.input).setLocality(Locality.CONTAINER_LOCAL);
  }
}