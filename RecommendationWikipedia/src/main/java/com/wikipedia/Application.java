/**
 * Put your copyright and license info here.
 */
package com.wikipedia;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

@ApplicationAnnotation(name="WikipediaApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
      ReadFile readFile = dag.addOperator("readFile",new ReadFile());
      CreateItemPair itemPair=dag.addOperator("cooccurrenceRow",new CreateItemPair());
      PairKeyCounter counter= dag.addOperator("Cooccurrences",new PairKeyCounter());
    //  ConsoleOutputOperator cons =dag.addOperator("console",ConsoleOutputOperator.class);
      WriteToFile writeToFile = dag.addOperator("FileWriter", new WriteToFile());
      BuildRecommendation buildR= dag.addOperator("buildR",new BuildRecommendation());
//1
      dag.addStream("ReadFile to createitempair " ,readFile.output,itemPair.hashInput);
      dag.addStream("readfile to Br",readFile.vector,buildR.userVector);
//2
      dag.addStream("createitempair to paircounter",itemPair.coOccures,counter.inputPort);
//3
      dag.addStream("pairCounter to Br",counter.outputPort, buildR.xyInput);
//4
      dag.addStream("Update Recommendations",buildR.Rout,writeToFile.input);

//@ignore
      //dag.addStream("pairCounter to console",counter.outputPort, cons.input);

  }
}