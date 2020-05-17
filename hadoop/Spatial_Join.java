import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class Spatial_Join {
    public static class Map_Join extends Mapper<Object, Text, Text, Text>{

        Map<String, String> points = new HashMap<String, String>();

        public void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream fis = fs.open(new Path("/user/mqp/project2/Rec.csv"));
            BufferedReader buffer = new BufferedReader(new InputStreamReader(fis));
            String line = buffer.readLine();
            int k = 0;
            while(line != null){
                String[] record = line.split(",");
                String name = record[0];
                int x = Integer.parseInt(record[1]);
                int y = Integer.parseInt(record[2]);
                int width = Integer.parseInt(record[3]);
                int height = Integer.parseInt(record[4]);
                for(int i = 0; i <= width; i++){
                    for(int j = 0; j <= height; j++){
                        String point = "(" + String.valueOf(x + i) + "," + String.valueOf(x + j) + ")";
                        String result = points.getOrDefault(point, "");
                        result += (name + "&");
                        points.put(point, result);
                    }
                }
                line = buffer.readLine();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            String point_info = "(" + record[0] + "," + record[1] + ")";
            Text rec = new Text();
            Text point = new Text();
            String recs = points.get(point_info);
            if(recs != null){
                String[] rec_infos = recs.split("&");
                for(String rec_info : rec_infos){
                    rec.set(rec_info);
                    point.set(point_info);
                    context.write(rec, point);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Spatial_Join.class);
        job.setMapperClass(Map_Join.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/mqp/project2/Points.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/mqp/project2/output1/"));
        job.waitForCompletion(true);
    }
}
