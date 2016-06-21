/**
 * Put your copyright and license info here.
 */
package WordCount;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Add Operator
    LineReader lr= dag.addOperator("LineReader",LineReader.class);
    Parser  parser= dag.addOperator("Parser",Parser.class);
    UniqueCounter<String> counter=dag.addOperator("Counter",new UniqueCounter<String>());
    ConsoleOutputOperator console = dag.addOperator("Console",new ConsoleOutputOperator());
    //Add Streams
    console.setSilent(false);
    console.setDebug(true);
    dag.addStream("LineReader to Parser",lr.output,parser.input);
    dag.addStream("Parser to Counter",parser.output,counter.data);
    dag.addStream("Counter to Console",counter.count,console.input);
  }
}
