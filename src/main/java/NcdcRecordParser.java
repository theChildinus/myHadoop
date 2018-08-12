import org.apache.hadoop.io.Text;

import java.text.*;
import java.util.Date;

public class NcdcRecordParser {
    private static final int MISSING_TEMPERATURE = 9999;

    private String stationId;
    private String observationsDateString;
    private String year;
    private String airTemperatureString;
    private int airTemperature;
    private boolean airTemperatureMalformed;
    private String quality;

    public void parse(String record) {
        stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
        observationsDateString = record.substring(15, 17);
        year = record.substring(15, 19);
        airTemperatureMalformed = false;

        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else if (record.charAt(87) == '-') {
            airTemperatureString = record.substring(87, 92);
            airTemperature = Integer.parseInt(airTemperatureString);
        } else {
            airTemperatureMalformed = true;
        }

        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    public boolean isMalformedTemperature() { return airTemperatureMalformed; }

    public boolean isMissingTemperature() { return airTemperature == MISSING_TEMPERATURE; }

    public String getYear() { return year; }

    public int getYearInt() { return Integer.parseInt(year); }

    public int getAirTemperature() { return airTemperature; }

    public String getAirTemperatureString() { return airTemperatureString; }

    public String getStationId() { return stationId; }

    public String getQuality() { return quality; }
}
