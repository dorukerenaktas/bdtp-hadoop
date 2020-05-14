package model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OHLCTuple implements Writable {

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private Date date;
    private double open;
    private double high;
    private double low;
    private double close;
    private double volume;

    public OHLCTuple(String date, double open, double high, double low, double close, double volume) {
        try {
            this.date = DATE_FORMAT.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }

    public Date getDate() {
        return date;
    }

    public double getOpen() {
        return open;
    }

    public double getHigh() {
        return high;
    }

    public double getLow() {
        return low;
    }

    public double getClose() {
        return close;
    }

    public double getVolume() {
        return volume;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(DATE_FORMAT.format(date));
        dataOutput.writeDouble(open);
        dataOutput.writeDouble(high);
        dataOutput.writeDouble(low);
        dataOutput.writeDouble(close);
        dataOutput.writeDouble(volume);
    }

    public void readFields(DataInput dataInput) throws IOException {
        try {
            date = DATE_FORMAT.parse(dataInput.readLine());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        open = dataInput.readDouble();
        high = dataInput.readDouble();
        low = dataInput.readDouble();
        close = dataInput.readDouble();
        volume = dataInput.readDouble();
    }
}
