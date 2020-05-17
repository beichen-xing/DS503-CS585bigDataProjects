import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class K_Means {

    public static int iter = 0;
    public static double[][] ini_points = new double[1000][2];
    public final static double thres = 0.1;

    public static double distance(double[] point1, double[] point2){
        double distance = 0;
        for(int i = 0; i < point1.length; i++){
            distance += Math.pow((point1[i] - point2[i]), 2);
        }
        return distance;
    }

    public static class Map_KM extends Mapper<Object, Text, Text, Text>{

        private Text cluster_num = new Text();
        private Text info = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            double[] point = new double[2];
            point[0] = Double.parseDouble(record[0]);
            point[1] = Double.parseDouble(record[1]);
            String point_info = record[0] + "," + record[1] + ",1";
            double min = Double.MAX_VALUE;
            String cluster = "";
            for(int i = 0; i < 1000; i++){
                if(distance(point, ini_points[i]) < min) {
                    min = distance(point, ini_points[i]);
                    cluster = String.valueOf(i + 1);
                }
            }
            cluster_num.set(cluster);
            info.set(point_info);
            context.write(cluster_num, info);
        }
    }

    public static class combiner extends Reducer<Text, Text, Text, Text>{

        private Text info = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            double x_sum = 0;
            double y_sum = 0;
            int count = 0;
            for(Text value : values){
                x_sum += Double.parseDouble(value.toString().split(",")[0]);
                y_sum += Double.parseDouble(value.toString().split(",")[1]);
                count += Integer.parseInt(value.toString().split(",")[2]);
                /*if(key.toString().equals("30")) {
                    System.out.println(value.toString().split(",")[0]);
                    System.out.println(x_sum);
                    System.out.println(count);
                }*/
            }
            info.set(String.valueOf(x_sum) + "," + y_sum + "," + count);
            if(key.toString().equals("30"))
                System.out.println(key.toString() + info.toString());
            context.write(key, info);
        }
    }

    public static class Reduce_KM extends Reducer<Text, Text, Text, Text>{

        private Text centroid = new Text();

        public Reduce_KM(){
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            double x_sum = 0;
            double y_sum = 0;
            double x = 0;
            double y = 0;
            int count = 0;
            for(Text value : values){
                String[] info = value.toString().split(",");
                if(key.toString().equals("30")){
                    System.out.println(value.toString());
                }
                x_sum += Double.parseDouble(info[0]);
                y_sum += Double.parseDouble(info[1]);
                count += Integer.parseInt(info[2]);
            }
            //System.out.println(key.toString() + count);
            x = x_sum / count;
            y = y_sum / count;
            centroid.set(String.valueOf(x) + "," + String.valueOf(y));
            context.write(key, centroid);
        }
    }

    public static class Map_result extends Mapper<Object, Text, Text, Text>{
        private Text centroid = new Text();
        private Text info = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] record = value.toString().split(",");
            double[] point = new double[2];
            point[0] = Double.parseDouble(record[0]);
            point[1] = Double.parseDouble(record[1]);
            String point_info = "(" + record[0] + "," + record[1] + ")";
            double min = Double.MAX_VALUE;
            int cluster = 0;
            for(int i = 0; i < 1000; i++){
                if(distance(point, ini_points[i]) < min) {
                    min = distance(point, ini_points[i]);
                    cluster = i + 1;
                }
            }
            centroid.set("(" + ini_points[cluster-1][0] + "," + ini_points[cluster-1][1] + ")");
            info.set(point_info);
            context.write(centroid, info);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fis;
        fs.delete(new Path("/user/mqp/project2/output3"), true);
        fs.delete(new Path("/user/mqp/project2/output3_final"), true);
        while(iter < 2) {

            System.out.println("***********" + (iter+1) + " iteration starts!" + "************");

            double x_old = 0;
            double y_old = 0;
            double x_new = 0;
            double y_new = 0;
            double diff_x = 0;
            double diff_y = 0;
            conf = new Configuration();
            conf.set("mapred.textoutputformat.separator", ",");
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            fs = FileSystem.get(conf);
            if(iter == 0)
                fis = fs.open(new Path("/user/mqp/project2/initial.csv"));
            else
                fis = fs.open(new Path("/user/mqp/project2/output3/part-r-00000"));
            BufferedReader buffer = new BufferedReader(new InputStreamReader(fis));
            String line = buffer.readLine();
            int i = 0;
            while(line != null){
                ini_points[i][0] = Double.parseDouble(line.split(",")[1]);
                ini_points[i][1] = Double.parseDouble(line.split(",")[2]);
                x_old += ini_points[i][0];
                y_old += ini_points[i][1];
                i++;
                line = buffer.readLine();
            }
            fs.delete(new Path("/user/mqp/project2/output3"), true);

            Job job = Job.getInstance(conf);
            job.setJarByClass(K_Means.class);
            job.setMapperClass(Map_KM.class);
            job.setCombinerClass(combiner.class);
            job.setReducerClass(Reduce_KM.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/mqp/project2/Points.csv"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/mqp/project2/output3/"));
            job.waitForCompletion(true);

            fis = fs.open(new Path("/user/mqp/project2/output3/part-r-00000"));
            buffer = new BufferedReader(new InputStreamReader(fis));
            line = buffer.readLine();
            i = 0;
            while(line != null){
                x_new += Double.parseDouble(line.split(",")[1]);
                y_new += Double.parseDouble(line.split(",")[2]);
                i++;
                line = buffer.readLine();
            }
            diff_x = Math.abs(x_new - x_old)/1000;
            diff_y = Math.abs(y_new - y_old)/1000;
            if(diff_x <= thres && diff_y <= thres) {
                System.out.println("***********" + "Threshold has been reached at " + iter + "iteration!" + "************");
                break;
            }

            System.out.println("***********" + (iter+1) + " iteration ends!" + "************");
            iter++;
        }
        if(iter == 50)
            System.out.println("***********" + "Max Iteration has been reached." + "************");

        System.out.println("***********" + "Generate final result." + "************");

        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        fs = FileSystem.get(conf);
        if(iter == 0)
            fis = fs.open(new Path("/user/mqp/project2/initial.csv"));
        else
            fis = fs.open(new Path("/user/mqp/project2/output3/part-r-00000"));
        BufferedReader buffer = new BufferedReader(new InputStreamReader(fis));
        String line = buffer.readLine();
        int i = 0;
        while(line != null){
            ini_points[i][0] = Double.parseDouble(line.split(",")[1]);
            ini_points[i][1] = Double.parseDouble(line.split(",")[2]);
            i++;
            line = buffer.readLine();
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(K_Means.class);
        job.setMapperClass(Map_result.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/mqp/project2/Points.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/mqp/project2/output3_final/"));
        job.waitForCompletion(true);
    }
}
