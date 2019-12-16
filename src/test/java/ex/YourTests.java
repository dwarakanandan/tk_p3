package ex;

import ex.deserialization.FlightParser;
import ex.deserialization.FlightParserImpl;
import ex.deserialization.objects.Flight;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class YourTests {

    static AirportInfo uut;
    static FlightParser fput;
    static final String FLIGHTS_PATH = "./Fraport/2018-08-0*";
    static Dataset<Flight> flights;

    @BeforeAll
    static void init() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        uut = new AirportInfoImpl();
        fput = new FlightParserImpl();
        flights = fput.parseFlights(FLIGHTS_PATH);
    }

    @Nested
    @DisplayName("Task 5")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class FlightsOfAirlineWithStatus {

        List<String> testListOfFlights;

        @BeforeAll
        @Timeout(30)
        void setup() {
            flights = fput.parseFlights(FLIGHTS_PATH);
        }

        @Test
        @DisplayName("Test with a single filter")
        void oneFilter() {
            Dataset<Flight> result = uut.flightsOfAirlineWithStatus(flights,"DE", "S");
            result.show(false);
            assertTrue(true);
        }

        @Test
        @DisplayName("Test with a single filter")
        void multiFilter() {
            Dataset<Flight> result = uut.flightsOfAirlineWithStatus(flights,"DE", "S", "X");
            result.show(false);
            assertTrue(true);
        }

    }

    @Nested
    @DisplayName("Task 6")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class AvgNumberOfFlightsInWindow {

        List<String> testListOfFlights;

        @BeforeAll
        @Timeout(30)
        void setup() {
        }

        @Test
        @DisplayName("Test 1")
        void testOne() {
            String start = "03:30:00";
            String end = "04:00:00";
            double result = uut.avgNumberOfFlightsInWindow(flights, start, end);
            System.out.println("Average number of flights between " + start + " and " + end + ": " + result);
            assertTrue(true);
        }

        @Test
        @DisplayName("Test 2")
        void testTwo() {
            String start = "12:00:00";
            String end = "18:00:00";
            double result = uut.avgNumberOfFlightsInWindow(flights, start, end);
            System.out.println("Average number of flights between " + start + " and " + end + ": " + result);
            assertTrue(true);
        }

    }
}
