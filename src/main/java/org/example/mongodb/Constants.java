package org.example.mongodb;

public class Constants {
	
	public static final String DB_NAME_LOGISTICS = "logistics";
	public static final String COLLECTION_CITIES = "cities";
	public static final String COLLECTION_PLANES = "planes";
	public static final String COLLECTION_CARGO = "cargo";
	public static final String _ID = "_id";
	public static final String SCHEMA_VERSION = "schemaVersion";

	
	// *** PLANES ***
	public static final String HEADING = "heading";
	public static final String CURRENT_LOCATION = "currentLocation";
	public static final String ROUTE = "route";
	public static final String LANDED = "landed";
	public static final String CALLSIGN = "callsign";
	
	public static final int MAINTENANCE_REQUIRED_LIMIT = 50000;  //in Kms
	public static final char UNIT_MILES = 'M';
	
	
	public static final String LAST_LANDING_EVENT = "lastLandingEvent";
	public static final String REQUIRES_MAINTENANCE = "requiresMaintenance";
	public static final String TOTAL_DISTANCE_FLOWN = "totalDistanceFlown";
	public static final String TOTAL_FLIGHT_TIME = "totalFlightTime";
	public static final String FLIGHT_LOG = "flightLog";
	
	// *** PLANE FLIGHT LOG ***
	public static final String LANDING_LOCATION = "location";
	public static final String LANDING_DATE = "date";
	
	// *** CITIES ***
	public static final String NAME = "name";
	public static final String COUNTRY = "country";
	public static final String POSITION = "position";

	// *** CARGO ***
	public static final String CARGO_ID = "id";
	public static final String DESTINATION = "destination";
	public static final String LOCATION = "location";
	public static final String COURIER = "courier";
	public static final String RECEIVED = "received";
	public static final String DELIVERED = "delivered";	
	public static final String STATUS = "status";
	
	public static final String STATUS_INPROCESS = "in process";
	public static final String STATUS_INTRANSIT = "in transit";
	public static final String STATUS_DELIVERED = "delivered";	

}
