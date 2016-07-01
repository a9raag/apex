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
      CreateItemPair itemPair=dag.addOperator("cooccurrenceRow",new CreateItemPair());
      RecommendationCounter counter= dag.addOperator("Cooccurrences",new RecommendationCounter());
      counter.setCumulative(true);
      WriteToFile writeToFile = dag.addOperator("FileWriter", new WriteToFile());
      BuildRecommendation buildR= dag.addOperator("buildR",new BuildRecommendation());


//1
      dag.addStream("Read The File",readFile.output,itemPair.hashInput);
      dag.addStream("UserVectors",readFile.vector,buildR.userVector);
//2
      dag.addStream("Produce Cooccurrence",itemPair.coOccures,counter.data);
//3
      dag.addStream("BuildR from Cooccurrences",counter.count,buildR.xyInput).setLocality(DAG.Locality.THREAD_LOCAL);
//4
      dag.addStream("Update Recommendations",buildR.Rout,writeToFile.input);

//    dag.addStream("randomData", randomGenerator.out, cons.input).setLocality(Locality.CONTAINER_LOCAL);
  }
}