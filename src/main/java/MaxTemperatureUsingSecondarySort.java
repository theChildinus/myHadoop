import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import java.io.IOException;

public class MaxTemperatureUsingSecondarySort extends Configured implements Tool {

    static class MaxTemperatureMapper
            extends Mapper<LongWritable, Text, IntPair, NullWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            parser.parse(value);
            // 用IntPair类定义了一个代表年份和气温的组合键
            // 解析Text，生成IntPair作为中间输出的key
            if (parser.isValidTemperature()) {
                context.write(new IntPair(parser.getYearInt(), parser.getAirTemperature()), NullWritable.get());
            }
        }
    }

    static class MaxTemperatureReducer
            extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {

        // reudcer只负责把数据直接输出，没有对中间数据的处理，
        // 但是如果没有reducer，中间结果会直接写到最终结果，没有group的过程了
        @Override
        protected void reduce(IntPair key, Iterable<NullWritable> value, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    // 自定的partitioner 以按照组合键的首字段（年份）进行分区
    public static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {

        @Override
        public int getPartition(IntPair key, NullWritable value, int numPartitions) {
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    // 中间输出结果按照该比较器进行排序，key比较器决定了key的排序方式
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            // 第一个参数相等的前提下才比较第二个参数
            int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
            if (cmp != 0) {
                return cmp;
            }
            return -IntPair.compare(ip1.getSecond(), ip2.getSecond());
        }
    }


    // 中间结果输出在reducer的context上，通过该比较器进行判断
    // 中间输出经过上一部已经是按序排列的，所以本例中输入第一列相同的IntPair会被排列在一起
    // group比较器决定了同一组中哪个数据最终可以被输出
    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(IntPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            return IntPair.compare(ip1.getFirst(), ip2.getFirst());
        }
    }

    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        job.setMapperClass(MaxTemperatureMapper.class);

        job.setPartitionerClass(FirstPartitioner.class);

        // 为了按照年份升序，气温降序排列键，我们使用setSortComparatorClass设置一个自定义键comparator以抽取字段并执行比较操作
        job.setSortComparatorClass(KeyComparator.class);

        // 为了按照年份对键进行分组，我们使用SetGroupComparatorClass来自定一个comparator，只取键的首字段进行比较
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
        System.exit(exitCode);
    }
}
