package ex;

import ex.deserialization.objects.Flight;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;
import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

public class AirportInfoImpl implements AirportInfo {

    /**
     * Usage of some example operations.
     *
     * @param flights dataframe of flights
     */
    public void sparkExample(Dataset<Row> flights) {
        System.out.println("Example printSchema");
        flights.printSchema();

        System.out.println("Example select and filter");
        // operations on a dataframe or rdd do not modify it, but return a new one
        Dataset<Row> selectAirlineDisplayCode = flights.select("flight.operatingAirline.iataCode", "flight.aircraftType.icaoCode").filter(r -> !r.anyNull());
        selectAirlineDisplayCode.show(false);

        System.out.println("Example groupBy and count");
        Dataset<Row> countOfAirlines = selectAirlineDisplayCode.groupBy("iataCode").count();
        countOfAirlines.show(false);

        System.out.println("Example where");
        Dataset<Row> selectOnlyLufthansa = selectAirlineDisplayCode.where("iataCode = 'LH'");
        selectOnlyLufthansa.show(false);

        System.out.println("Example map to String");
        Dataset<String> onlyAircraftIcaoCodeAsString = selectOnlyLufthansa.map(r -> r.getString(1), Encoders.STRING());
        onlyAircraftIcaoCodeAsString.show(false);

        System.out.println("Example mapToPair and reduceByKey");
        JavaRDD<Row> rdd = selectOnlyLufthansa.toJavaRDD();
        JavaPairRDD<String, Long> paired = rdd.mapToPair(r -> Tuple2.apply(r.getString(1), 1L));
        JavaPairRDD<String, Long> reducedByKey = paired.reduceByKey((a, b) -> a + b);
        reducedByKey.take(20).forEach(t -> System.out.println(t._1() + " " + t._2()));
    }

    /**
     * Task 1
     * Return a dataframe, in which every row contains the destination airport and its count over all available data
     * of departing flights, sorted by count in descending order.
     * The column names are (in this order):
     * arrivalAirport | count
     * Remove entries that do not contain an arrivalAirport member (null or empty).
     * Use the given Dataframe.
     *
     * @param departingFlights Dataframe containing the rows of departing flights
     * @return a dataframe containing statistics of the most common destinations
     */
    @Override
    public Dataset<Row> mostCommonDestinations(Dataset<Row> departingFlights) {
        // Filter the arrivalAirport to get only those that are not-null or non-empty strings
        Dataset<Row> arrivalAirports = departingFlights.select("flight.arrivalAirport").filter(departingFlights.col("flight.arrivalAirport").isNotNull()).filter(departingFlights.col("flight.arrivalAirport").notEqual(""));
        // Group-by arrivalAirport and get count
        Dataset<Row> countOfArrivalAirports = arrivalAirports.groupBy("arrivalAirport").count();
        // Sort the above data frame in descending order of count
        Dataset<Row> sortedCountOfArrivalAirports = countOfArrivalAirports.orderBy(col("count").desc());
        System.out.println("Task 1 - mostCommonDestinations");
        sortedCountOfArrivalAirports.show();
        return sortedCountOfArrivalAirports;
    }

    /**
     * Task 2
     * Return a dataframe, in which every row contains a gate and the amount at which that gate was used for flights to
     * Berlin ("count"), sorted by count in descending order. Do not include gates with 0 flights to Berlin.
     * The column names are (in this order):
     * gate | count
     * Remove entries that do not contain a gate member (null or empty).
     * Use the given Dataframe.
     *
     * @param departureFlights Dataframe containing the rows of departing flights
     * @return dataframe with statistics about flights to Berlin per gate
     */
    @Override
    public Dataset<Row> gatesWithFlightsToBerlin(Dataset<Row> departureFlights) {
        Dataset<Row> flightsToBerlin = departureFlights.select("flight.departure.gates")
            .filter(departureFlights.col("flight.arrivalAirport").isNotNull())
            .filter(departureFlights.col("flight.arrivalAirport").equalTo("TXL"));
        Dataset<Row> gatesFlightsToBerlin = flightsToBerlin.withColumn("gates", explode(flightsToBerlin.col("gates")));
        Dataset<Row> countGatesFlightsToBerlin = gatesFlightsToBerlin.groupBy("gates.gate").count();
        Dataset<Row> sortedCountGatesFlightsToBerlin = countGatesFlightsToBerlin.orderBy(col("count").desc());
        System.out.println("Task 2 - gatesWithFlightsToBerlin");
        sortedCountGatesFlightsToBerlin.show();
        return sortedCountGatesFlightsToBerlin;
    }

    /**
     * Task 3
     * Return a JavaPairRDD with String keys and Long values, containing count of flights per aircraft on the given
     * originDate. The String keys are the modelNames of each aircraft and their Long value is the amount of flights for
     * that modelName at the given originDate. Do not include aircrafts with 0 flights.
     * Remove entries that do not contain a modelName member (null or empty).
     * The date string is of the form 'YYYY-MM-DD'.
     * Use the given dataframe.
     *
     * @param flights    Dataframe containing the rows of flights
     * @param originDate the date to find the most used aircraft for
     * @return tuple containing the modelName of the aircraft and its total number
     */
    @Override
    public JavaPairRDD<String, Long> aircraftCountOnDate(Dataset<Row> flights, String originDate) {
        Dataset<Row> flightsOnOriginDate = flights.select("flight.aircraftType.modelName")
            .filter(flights.col("flight.originDate").equalTo(originDate))
            .filter(flights.col("flight.aircraftType.modelName").isNotNull())
            .filter(flights.col("flight.aircraftType.modelName").notEqual(""));

        Dataset<Row> countFlightsOnOriginDate = flightsOnOriginDate.groupBy("modelName").count().orderBy(col("count").desc());
        System.out.println("Task 3 - aircraftCountOnDate");
        countFlightsOnOriginDate.show();
        JavaRDD<Row> rdd = countFlightsOnOriginDate.toJavaRDD();
        JavaPairRDD<String, Long> returnRdd = rdd.mapToPair(r -> Tuple2.apply(r.getString(0), r.getLong(1)));
        return returnRdd;
    }

    /**
     * Task 4
     * Returns the date string of the day at which Ryanair had a strike in the given Dataframe.
     * The returned string is of the form 'YYYY-MM-DD'.
     * Hint: There were strikes at two days in the given period of time. Both are accepted as valid results.
     *
     * @param flights Dataframe containing the rows of flights
     * @return day of strike
     */
    @Override
    public String ryanairStrike(Dataset<Row> flights) {
        
        Dataset<Row> ryanairFlights = flights.select("flight.originDate","flight.flightStatus")
            .filter(flights.col("flight.operatingAirline.name").equalTo("Ryanair"));
        
        // Iterate over all days in August
        for (int i = 1; i <= 31; i++) {
            String date = "2018-08-";
            if(i<10) {
                date+= "0" + i;
            } else {
                date+= i;
            }
            if(areAllFlightsCancelledToday(ryanairFlights, date)) return date;
        }

        // Iterate over all days in September
        for (int i = 1; i <= 30; i++) {
            String date = "2018-09-";
            if(i<10) {
                date+= "0" + i;
            } else {
                date+= i;
            }
            if(areAllFlightsCancelledToday(ryanairFlights, date)) return date;
        }

        return null;
    }

    // If Flight count on given currentDate is greater than 1 and all flights have flightStatus "X" returns true
    private boolean areAllFlightsCancelledToday(Dataset<Row> ryanairFlights, String currentDate) {
        Dataset<Row> flightsOnCurrentDate = ryanairFlights.select("flightStatus")
            .filter(ryanairFlights.col("originDate").equalTo(currentDate))
            .filter(ryanairFlights.col("flightStatus").isNotNull());
        long totalFlightsOnCurrentDate = flightsOnCurrentDate.count();
        if (totalFlightsOnCurrentDate <= 1) {
            return false;
        }

        Dataset<Row> cancelledFlightsOnCurrentDate = flightsOnCurrentDate.select("flightStatus")
            .filter(ryanairFlights.col("flightStatus").equalTo("X"));
        long totalCancelledFlightsOnCurrentDate = cancelledFlightsOnCurrentDate.count();
        if (totalCancelledFlightsOnCurrentDate == totalFlightsOnCurrentDate) {
            return true;
        }
        return false;
    }

    /**
     * Task 5
     * Returns a dataset of Flight objects. The dataset only contains flights of the given airline with at least one
     * of the given status codes. Uses the given Dataset of Flights.
     *
     * @param flights            Dataset containing the Flight objects
     * @param airlineDisplayCode the display code of the airline
     * @param status1            the status code of the flight
     * @param status             more status codes
     * @return dataset of Flight objects matching the required fields
     */
    @Override
    public Dataset<Flight> flightsOfAirlineWithStatus(Dataset<Flight> flights, String airlineDisplayCode, String status1, String... status) {
        Column filterAirlineCodeColumn = flights.col("airlineDisplayCode").equalTo(airlineDisplayCode);
        Column filterStatusColumn = flights.col("flightStatus").equalTo(status1);
        for(String s: status){
            filterStatusColumn = filterStatusColumn.or(flights.col("flightStatus").equalTo(s));
        }

        Column filter = filterAirlineCodeColumn.and(filterStatusColumn);
        System.out.println(filter);

        Dataset<Flight> filteredFlights = flights.filter(filter);


        return filteredFlights;
    }

    /**
     * Task 6
     * Returns the average number of flights per day between the given timestamps (both included).
     * The timestamps are of the form 'hh:mm:ss'. Uses the given Dataset of Flights.
     * Hint: You only need to consider "scheduledTime" for this. Do not include flights with
     * empty "scheduledTime" field. You can assume that lowerLimit is always before or equal
     * to upperLimit.
     *
     * @param flights Dataset containing the arriving Flight objects
     * @param lowerLimit     start timestamp (included)
     * @param upperLimit     end timestamp (included)
     * @return average number of flights between the given timestamps (both included)
     */
    @Override
    public double avgNumberOfFlightsInWindow(Dataset<Flight> flights, String lowerLimit, String upperLimit) {
        Dataset<Flight> flightsInPeriod = flights.filter(flight -> {
            String flightTime = flight.getScheduledTime();
            if(flightTime == "") return false;

            return flightTime.compareTo(lowerLimit) >= 0 && flightTime.compareTo(upperLimit) <= 0;
        });

        MapFunction<Flight, String> map = flight -> {
            return flight.getScheduled();
        };

        KeyValueGroupedDataset<String, Flight> groupByDay = flightsInPeriod.groupByKey(map, Encoders.STRING());

        Dataset<Tuple2<String, Object>> count = groupByDay.count();
        Dataset<Row> avgDataset = count.agg(avg("Count(1)"));
        double avg = (double)avgDataset.first().get(0);
        return avg;
    }

    /**
     * Returns true if the first timestamp is before or equal to the second timestamp. Both timestamps must match
     * the format 'hh:mm:ss'.
     * You can use this for Task 6. Alternatively use LocalTime or similar API (or your own custom method).
     *
     * @param before the first timestamp
     * @param after  the second timestamp
     * @return true if the first timestamp is before or equal to the second timestamp
     */
    private static boolean isBefore(String before, String after) {
        for (int i = 0; i < before.length(); i++) {
            char bef = before.charAt(i);
            char aft = after.charAt(i);
            if (bef == ':') continue;
            if (bef < aft) return true;
            if (bef > aft) return false;
        }

        return true;
    }
}
