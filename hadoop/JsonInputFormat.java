import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class JsonInputFormat extends TextInputFormat {
    public static final String left = "{";
    public static final String right = "}";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new JsonRecordReader();
    }

    public static class JsonRecordReader extends RecordReader<LongWritable, Text> {

        private byte[] leftTag;
        private byte[] rightTag;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();

// convert Json to Line
        public Text ToLine(Text value) {
            String v = value.toString().trim();
            String[] vList = v.substring(1, v.length()-1).trim().split(",");

            StringBuilder sb = new StringBuilder();
            for(String item: vList){
                sb.append(item.trim());
                sb.append(",");
            }
            String result = sb.toString();
            result = result.substring(0,sb.length()-1);
            return new Text(result);
        }

// initial
        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) is;
            leftTag = left.getBytes(StandardCharsets.UTF_8);
            rightTag = right.getBytes(StandardCharsets.UTF_8);

            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();

            FileSystem fs = file.getFileSystem(tac.getConfiguration());
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);
        }
// check left tag or right tag
        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i = 0;
            while (true) {
                int val = fsin.read();
                if (val == -1)  return false;
                if (withinBlock) buffer.write(val);
                if (val == match[i]) {
                    i++;
                    if (i >= match.length) return true;
                } else{
                    i = 0;
                }
                if (!withinBlock && i == 0 && fsin.getPos() >= end)
                    return false;
            }
        }

// get the next key value
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fsin.getPos() < end) {
                // start
                if (readUntilMatch(leftTag, false)) {
                    try {
                        buffer.write(leftTag);
                        // In the end, Set Key and value
                        if (readUntilMatch(rightTag, true)) {
                            key.set(fsin.getPos());
                            value.set(buffer.getData(), 0, buffer.getLength());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return ToLine(value);

        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

    }
}