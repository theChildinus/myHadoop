import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class MissingTemperatureFilelds extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            JobBuilder.printUsage(this, "<job ID>");
            return -1;
        }
        String jobID = args[0];
        Cluster cluster = new Cluster(getConf());

        // 以作业ID为输入参数调用getJob方法，从Cluster中获取一个Job对象
        Job job = cluster.getJob(JobID.forName(jobID));
        if (job == null) {
            System.err.printf("No job with ID %s found\n", jobID);
            return -1;
        }

        if (!job.isComplete()) {
            System.err.printf("Job %s is not complete\n", jobID);
            return -1;
        }

        // 确认该作业已完成，调用getCounters方法返回一个Counters对象，封装了该作业的所有计数器
        Counters counters = job.getCounters();
        long missing = counters.findCounter(
                MaxTemperatureWithCounters.Temperature.MISSING).getValue();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();

        System.out.printf("Records with missing temperature fields: %.2f%%\n",
        100.0 * missing / total);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MissingTemperatureFilelds(), args);
        System.exit(exitCode);
    }
}
