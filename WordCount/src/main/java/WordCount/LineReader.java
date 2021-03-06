package WordCount;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by anurag on 9/6/16.
 */

public class LineReader extends AbstractFileInputOperator<String> {
    public transient final DefaultOutputPort<String> output =new DefaultOutputPort<String>();
    private transient BufferedReader br; //it won't be put in checkpoint or have them serialized

    @Override
    protected InputStream openFile(Path path) throws IOException {
        InputStream is=super.openFile(path);
        br= new BufferedReader(new InputStreamReader(is));
        return is;
    }

    @Override
    protected void closeFile(InputStream is) throws IOException {
        super.closeFile(is);
        br.close();
    }

    @Override
    protected String readEntity() throws IOException {

        return br.readLine();
    }

    @Override
    protected void emit(String s) {
        output.emit(s);
    }
}
