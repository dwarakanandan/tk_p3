package ex.deserialization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ex.deserialization.objects.Flight;
import ex.deserialization.objects.FlightObj;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;

import java.util.LinkedList;

public class FlightParserImpl implements FlightParser {

    private SparkSession sparkSession;

    public FlightParserImpl() {
        SparkConf conf = new SparkConf().setAppName("SparkExercise").setMaster("local");
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
    }

    /**
     * Returns a Dataframe, containing all objects from the JSON files in the given path.
     *
     * @param path the path containing (only) the JSON files
     * @return Dataframe containing the input data
     */
    @Override
    public Dataset<Row> parseRows(String path) {
        return sparkSession.read().json(path);
    }

    /**
     * Deserializes the JSON lines to Flight objects. Each String in the given Dataset represents a JSON object.
     * Use Gson for this.
     * Hints: Apply a map function (mapPartitions) on the dataset, in which you use Gson to deserialize each line into objects of FlightObj.
     * Use GsonBuilder to register the type adapter "FlightAdapter" for FlightObj and to create a Gson object.
     *
     * @param path the path to parse the jsons from
     * @return dataset of Flight objects parsed from the JSON lines
     */
    @Override
    public Dataset<Flight> parseFlights(String path) {
        Dataset<String> lines = sparkSession.sqlContext().read().textFile(path);

        MapPartitionsFunction<String, Flight> mapper = jsonFlights -> {
            GsonBuilder gsonBuilder = new GsonBuilder();
            gsonBuilder.registerTypeAdapter(FlightObj.class, new FlightAdapter());
            Gson gson = gsonBuilder.create();

            LinkedList<Flight> flights = new LinkedList<Flight>();
            String jsonFlight = "";
            while(jsonFlights.hasNext()){
                jsonFlight = jsonFlights.next();
                flights.add(gson.fromJson(jsonFlight, FlightObj.class).getFlight());
            }
            return flights.iterator();
        };

        Dataset<Flight> flightDataset = lines.mapPartitions(mapper, Encoders.bean(Flight.class));

        return flightDataset;
    }
}
