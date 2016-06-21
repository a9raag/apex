package WordCount;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by anurag on 9/6/16.
 */
public class Parser extends BaseOperator{
    public transient final DefaultInputPort<String> input = new DefaultInputPort<String>() {
        @Override
        public void process(String s) {
            String [] words = s.split(" ");
            for (String word: words){
                output.emit(word);
            }
        }
    };
    public transient final DefaultOutputPort<String> output= new DefaultOutputPort<>();
}
