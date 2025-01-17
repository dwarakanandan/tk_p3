package ex.deserialization.objects;

import java.io.Serializable;

public class Flight implements Serializable {

    private String airlineDisplayCode;
    private String departureAirport;
    private String arrivalAirport;
    private String originDate;
    private String flightStatus;
    private String scheduled;

    public String getAirlineDisplayCode() {
        if (airlineDisplayCode == null) return "";
        return airlineDisplayCode;
    }

    public void setAirlineDisplayCode(String airlineDisplayCode) {
        this.airlineDisplayCode = airlineDisplayCode;
    }

    public String getDepartureAirport() {
        if (departureAirport == null) return "";
        return departureAirport;
    }

    public String getArrivalAirport() {
        if (arrivalAirport == null) return "";
        return arrivalAirport;
    }

    public String getOriginDate() {
        if (originDate == null) return "";
        return originDate;
    }

    public String getFlightStatus() {
        if (flightStatus == null) return "";
        return flightStatus;
    }

    public String getScheduledTime() {
        if (scheduled == null || scheduled.isEmpty()) return "";

        return scheduled.substring(11, 19);
    }

    public String getScheduled() {
        if (scheduled == null) return "";
        return scheduled;
    }

    public void setScheduled(String scheduled) {
        this.scheduled = scheduled;
    }

    public String toString() {
        return airlineDisplayCode + "|" + departureAirport + "|" + arrivalAirport + "|" + originDate + "|" + flightStatus + "|" + scheduled;
    }

    public void setFlightStatus(String flightStatus) {
        this.flightStatus = flightStatus;
    }
}
