import com.opencsv.CSVParser;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ClosingFuture.Combiner;
import org.xbill.DNS.Update;

public class FindFlights {
  private static final String ORIGIN = "ORD";
  private static final String DESTINATION = "JFK";
  private static final int START_YEAR = 2007;
  private static final int END_YEAR = 2008;
  private static final int START_MONTH = 6;
  private static final int END_MONTH = 5;
  static enum UpdateCount{CNT1, CNT2}

  public static class Date implements WritableComparable<Date>{
    private IntWritable month;
    private IntWritable year;
    private IntWritable day;

    public Date() {
      this.year = new IntWritable();
      this.month = new IntWritable();
      this.day = new IntWritable();
    }

    public IntWritable getMonth() {
      return month;
    }

    public IntWritable getYear() {
      return year;
    }


    public IntWritable getDay() {
      return day;
    }

    public void setDate(Integer year, Integer month, Integer day) {
      this.year.set(year);
      this.month.set(month);
      this.day.set(day);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      this.year.write(dataOutput);
      this.month.write(dataOutput);
      this.day.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.year.readFields(dataInput);
      this.month.readFields(dataInput);
      this.day.readFields(dataInput);
    }

    @Override
    public int compareTo(Date o) {
      int yearVal = this.year.compareTo(o.getYear());
      if(yearVal != 0){
        return yearVal;
      }
      int monthVal = this.month.compareTo(o.getMonth());
      if(monthVal != 0){
        return monthVal;
      }
      return this.day.compareTo(o.getDay());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Date date = (Date) o;
      return Objects.equals(getMonth().get(), date.getMonth().get()) && Objects.equals(
          getYear().get(), date.getYear().get()) && Objects.equals(getDay().get()
          , date.getDay().get());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getMonth().get(), getYear().get(), getDay().get());
    }

    @Override
    public String toString() {
      return "Date{" +
          "month=" + month +
          ", year=" + year +
          ", day=" + day +
          '}';
    }
  }

  public static class Flight implements Writable {

    private Text origin;
    private Text destination;
    private IntWritable departureTime;
    private IntWritable arrivalTime;
    private DoubleWritable delay;

    public Flight() {
      origin = new Text();
      destination = new Text();
      departureTime = new IntWritable();
      arrivalTime = new IntWritable();
      delay = new DoubleWritable();
    }

    public Text getOrigin() {
      return origin;
    }

    public Text getDestination() {
      return destination;
    }

    public IntWritable getDepartureTime() {
      return departureTime;
    }

    public IntWritable getArrivalTime() {
      return arrivalTime;
    }

    public DoubleWritable getDelay() {
      return delay;
    }

    public void setFlight(String origin, String destination, Integer departureTime,
        Integer arrivalTime, Double delay) {
      this.origin.set(origin);
      this.destination.set(destination);
      this.departureTime.set(departureTime);
      this.arrivalTime.set(arrivalTime);
      this.delay.set(delay);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      this.origin.write(dataOutput);
      this.destination.write(dataOutput);
      this.departureTime.write(dataOutput);
      this.arrivalTime.write(dataOutput);
      this.delay.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.origin.readFields(dataInput);
      this.destination.readFields(dataInput);
      this.departureTime.readFields(dataInput);
      this.arrivalTime.readFields(dataInput);
      this.delay.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Flight flight = (Flight) o;
      return Objects.equals(getOrigin().toString(), flight.getOrigin().toString())
          && Objects.equals(getDestination().toString(), flight.getDestination().toString())
          && Objects.equals(getDepartureTime().get(), flight.getDepartureTime().get())
          && Objects.equals(getArrivalTime().get(), flight.getArrivalTime().get())
          && Objects.equals(getDelay().get(), flight.getDelay().get());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getOrigin().toString(), getDestination().toString(),
          getDepartureTime().get(), getArrivalTime().get(), getDelay().get());
    }

    @Override
    public String toString() {
      return "Flight{" +
          "origin=" + origin +
          ", destination=" + destination +
          ", departureTime=" + departureTime +
          ", arrivalTime=" + arrivalTime +
          ", delay=" + delay +
          '}';
    }
  }

  public static class FlightMapper
      extends Mapper<Object, Text, Date, Flight>{
    private CSVParser csvParser = new CSVParser();

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      String[] record = csvParser.parseLine(value.toString());
      Date date = new Date();
      Flight flight = new Flight();
      boolean cancelled = Double.parseDouble(record[41]) == 1.00;
      boolean diverted = Double.parseDouble(record[43]) == 1.00;
      int year = Integer.parseInt(record[0]);
      int month = Integer.parseInt(record[2]);
      String origin = record[11];
      String dest = record[17];
      if(!(cancelled || diverted)
          && ((year == START_YEAR && month >= START_MONTH)
          || (year == END_YEAR && month <= END_MONTH)) &&
          (origin.equals(ORIGIN) ^ dest.equals(DESTINATION))) {
        int day = Integer.parseInt(record[3]);
        int departureTime = Integer.parseInt(record[24]);
        int arrivalTime = Integer.parseInt(record[35]);
        double delay = Double.parseDouble(record[37]);
        date.setDate(year, month, day);
        flight.setFlight(origin, dest, departureTime, arrivalTime, delay);
        context.write(date, flight);
      }
    }
  }

  public static class FlightReducer
      extends Reducer<Date,Flight,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    private double sum = 0;
    private int pairs = 0;

    @Override
    public void reduce(Date key, Iterable<Flight> values,
        Context context
    ) throws IOException, InterruptedException {
      List<Flight> flights = new ArrayList<>();
      for (Flight f: values){
        flights.add(f);
      }
      for (Flight f1 : flights) {
        if(!f1.getOrigin().toString().equals(ORIGIN)) continue;
        for (Flight f2 : flights){
          if(f1.equals(f2)){
            continue;
          }
          if(f2.getOrigin().toString().equals(DESTINATION)
              && f1.getDestination().toString().equals(f2.getOrigin().toString())
              && f1.getArrivalTime().get() < f2.getDepartureTime().get()){
            sum += f1.getDelay().get() + f2.getDelay().get();
            pairs++;
          }
        }
      }
    }

    @Override
    protected void cleanup(Reducer<Date, Flight, Text, DoubleWritable>.Context context)
        throws IOException, InterruptedException {
      result.set(sum/pairs);
      context.write(new Text("Hi"), result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "find average delay");
    job.setJarByClass(FindFlights.class);
    job.setMapperClass(FlightMapper.class);
    job.setReducerClass(FlightReducer.class);
    job.setMapOutputKeyClass(Date.class);
    job.setMapOutputValueClass(Flight.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
