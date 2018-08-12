import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.awt.image.ImagingOpException;
import java.io.IOException;

public class v3MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    enum Temperature {
        OVER_100
    }

    private NcdcRecordParser parser = new NcdcRecordParser();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
            int airTemperature = parser.getAirTemperature();
            // 100度表示为1000
            if (airTemperature > 1000) {
                // 标识有问题的行
                System.err.println("Temperature over 100 for input:" + value);
                // 更新map状态
                context.setStatus("detected possibly corrupt record: see logs");
                // 计数器，统计超过100度的记录数
                context.getCounter(Temperature.OVER_100).increment(1);
            }
            context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
        }
    }
}
