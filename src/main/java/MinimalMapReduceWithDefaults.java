import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MinimalMapReduceWithDefaults extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        // 通过把打印使用说明的逻辑抽取出来，并把输入输出路径放到一个帮助方法中，实现对run()方法前几行简化
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        // 默认的输入格式是 TextInputFormat
        // 产生的键类型为 LongWritable（文件中每行中开始的偏移量值） 值类型是 Text（文本行）
        // 最后输出的整数含义: 行偏移量
        job.setInputFormatClass(TextInputFormat.class);

        // map 的输入输出键是LongWritable类型，输入输出值是Text类型
        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 它对每条记录的键进行Hash操作，以决定该记录属于哪个分区，
        // 每个分区由一个reduce任务处理，所以分区数等于作业的reduce任务个数
        job.setPartitionerClass(HashPartitioner.class);

        // map任务的数量等于输入文件被划分成的分块数
        // 目标reducer 保持在每个运行5分钟左右，且产生至少一个HDFS的输出比较合适
        job.setNumReduceTasks(1);
        job.setReducerClass(Reducer.class);

        // 大多数MapReduce程序不会一直用相同的键或值类型
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // 默认的输出格式为 TextOutputFormat
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
        System.exit(exitCode);
    }
}
