package org.example.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.popFirst;
import static com.mongodb.client.model.Updates.push;
import static com.mongodb.client.model.Updates.set;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.example.mongodb.util.DistanceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class PlaneDAL {

	Logger logger;
	private MongoClient mongoClient;
	private MongoCollection<Document> planesCollection;

	/**
	 * Since planes are to be created by a script (and not via API), there is no
	 * schema version 0 We will consider a document without schema version as valid,
	 * and start at 1
	 */
	private int schemaVersion = 1;
	private int doc_version = 0;

	private boolean populated;
	private String lastError;
	private String callSign;
	private ArrayList<Double> currentLocation;
	private Object heading;
	private ArrayList<String> route;
	private String status;
	private String landed;

	// Nobody else needs you, so no getters for you!
	private Document lastLandingEvent;
	private Double totalDistanceFlown;
	private Double totalFlightTime;
	private Boolean requiresMaintenance = false;

	public PlaneDAL(MongoClient mongoClient) {
		logger = LoggerFactory.getLogger(PlaneDAL.class);
		this.mongoClient = mongoClient;
		planesCollection = this.mongoClient.getDatabase(Constants.DB_NAME_LOGISTICS)
				.getCollection(Constants.COLLECTION_PLANES)
				.withWriteConcern(WriteConcern.MAJORITY)
				.withReadConcern(ReadConcern.MAJORITY);
		lastError = "";
	}

	// Find the plane, along with its last route, without the Flight Log
	public PlaneDAL(MongoClient mongoClient, String planeId) {
		this(mongoClient);

		/****
		 * Find the plane, using the right projection. It is important that we query
		 * only for fields we want to use, since the Flight Log can grow to an enormous
		 * value. We do not need the flight log here, leave it alone for Analytical
		 * queries.
		 * 
		 * We are, however, interested in the last landing event, and computed
		 * information. Hence, we get the last landing event from the Flight Log, and
		 * only retrieve that.
		 * 
		 */
		AggregateIterable<Document> result = planesCollection.aggregate(Arrays.asList(
				new Document("$match", new Document(Constants._ID, planeId)),
				new Document("$project", new Document(Constants._ID, 1L).append(Constants.CURRENT_LOCATION, 1L)
						.append(Constants.HEADING, 1L).append(Constants.ROUTE, 1L).append(Constants.LANDED, 1L)
						.append(Constants.STATUS, 1L).append(Constants.TOTAL_FLIGHT_TIME, 1L).append(Constants.SCHEMA_VERSION, 1L)
						.append(Constants.TOTAL_DISTANCE_FLOWN, 1L).append(Constants.LAST_LANDING_EVENT,
								new Document("$arrayElemAt", Arrays.asList("$" + Constants.FLIGHT_LOG, -1L))))));

		Document dbdata = result.first();

		if (dbdata != null) {
			parseDocument(dbdata);
		} else {
			lastError = String.format("Plane %s does not exist", planeId);
		}
	}

	private void parseDocument(Document doc) {
		populated = false;

		// We are expecting a null doc_version for planes added via script outside of
		// the APIs.
		if (null == doc.getInteger(Constants.SCHEMA_VERSION)) {
			doc_version = 0; // It's a valid case
		} else {
			doc_version = doc.getInteger(Constants.SCHEMA_VERSION);
		}

		switch (doc_version) {
		case 0: // Verion 0 is valid for plances added from the backend
			parseDocumentInitial(doc);
			break;
		case 1: // New version for Task 3
			parseDocumentV1(doc);
			break;
		}

	}

	@SuppressWarnings("unchecked")
	private void parseDocumentInitial(Document doc) {
		populated = false;
		try {
			callSign = doc.getString(Constants._ID);
			heading = doc.get(Constants.HEADING);
			currentLocation = (ArrayList<Double>) doc.get(Constants.CURRENT_LOCATION);
			route = (ArrayList<String>) doc.get(Constants.ROUTE);
			landed = doc.getString(Constants.LANDED);
			status = doc.getString(Constants.STATUS);

		} catch (Exception e) {
			lastError = e.getMessage();
			populated = false;
			return;
		}
		populated = true;
	}

	@SuppressWarnings("unchecked")
	private void parseDocumentV1(Document doc) {
		populated = false;
		try {
			callSign = doc.getString(Constants._ID);
			heading = doc.get(Constants.HEADING);
			currentLocation = (ArrayList<Double>) doc.get(Constants.CURRENT_LOCATION);
			route = (ArrayList<String>) doc.get(Constants.ROUTE);
			landed = doc.getString(Constants.LANDED);
			status = doc.getString(Constants.STATUS);

			// Added new properties for Task 3, only available in Schema Version 1
			lastLandingEvent = (Document) doc.get(Constants.LAST_LANDING_EVENT);
			totalDistanceFlown = doc.getDouble(Constants.TOTAL_DISTANCE_FLOWN);
			totalFlightTime = doc.getDouble(Constants.TOTAL_FLIGHT_TIME);
			requiresMaintenance = doc.getBoolean(Constants.REQUIRES_MAINTENANCE);

		} catch (Exception e) {
			lastError = e.getMessage();
			populated = false;
			return;
		}
		populated = true;
	}

	ArrayList<String> getAllPlanes() {
		ArrayList<String> allPlanes = new ArrayList<String>();

		try {
			MongoCursor<Document> resultsIterator = null;
			FindIterable<Document> results = planesCollection.find();

			for (resultsIterator = results.iterator(); resultsIterator.hasNext();) {
				Document document = resultsIterator.next();
				Document toReturn = new Document();
				toReturn.append(Constants.CALLSIGN, document.getString(Constants._ID));
				toReturn.append(Constants.CURRENT_LOCATION, document.get(Constants.CURRENT_LOCATION));
				toReturn.append(Constants.HEADING, document.get(Constants.HEADING));
				toReturn.append(Constants.ROUTE, document.get(Constants.ROUTE));
				toReturn.append(Constants.LANDED, document.get(Constants.LANDED));
				allPlanes.add(toReturn.toJson());
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return allPlanes;
	}

	boolean updateLocationHeadingLanding(String location, String heading, String landing) {

		if (isPopulated() == false) {
			return false;
		}

		if(!isValidCity(landing)) {
			lastError = String.format("City %s does not exist", landing);
			return false;
		}
		try {
			// findOneAndUpdate
			Bson filter = eq(Constants._ID, callSign);

			Bson updateLocation = set(Constants.CURRENT_LOCATION, createDoubleArrayFromString(location));
			Bson updateHeading = set(Constants.HEADING, heading);
			Bson updateLanding = set(Constants.LANDED, landing);
			Bson updateStatus = set(Constants.STATUS, "landed");

			Bson updates = combine(updateLocation, updateHeading, updateLanding, updateStatus);
			//Relying on retryable writes here.
			planesCollection.findOneAndUpdate(filter, updates);
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return false;
	}



	boolean updateLocationHeading(String location, String heading) {

		if (isPopulated() == false) {
			return false;
		}
		
		if(!isValidHeading(heading)) {
			return false;
		}
		if(!isValidLocation(location)) {
			return false;
		}

		try {
			// findOneAndUpdate
			Bson filter = eq(Constants._ID, callSign);

			Bson updateLocation = set(Constants.CURRENT_LOCATION, createDoubleArrayFromString(location));
			Bson updateHeading = set(Constants.HEADING, heading);

			Bson updates = combine(updateLocation, updateHeading);
			//Relying on retryable writes here.
			planesCollection.findOneAndUpdate(filter, updates);
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return false;
	}

	private boolean isValidLocation(String location) {
		boolean valid = false;
		if (location != null) {
			String values[] = location.split(",");
			if ((values != null) && (values.length == 2)) {
				try {
					Double.parseDouble(values[0]);
					Double.parseDouble(values[1]);
					valid = true;
				} catch (NumberFormatException e) {
					logger.error( "Invalid location value passed" +e.getMessage());
					lastError =  "Invalid location value passed" + e.getMessage();
				}
			}
		}
		return valid;
	}

	private boolean isValidHeading(String heading) {
		boolean valid = false;
		try {
			int intHeading = Integer.parseInt(heading);
			if ((0 <= intHeading) && (360 >= intHeading))
				valid = true;
		} catch (NumberFormatException e) {
			logger.error("Invalid heading value passed" + e.getMessage());
			lastError = "Invalid heading value passed" + e.getMessage();
		}
		return valid;
	}

	boolean addPlaneRoute(String route, Boolean isSingleCity) {

		if (isPopulated() == false) {
			return false;
		}
		if(!isValidCity(route)) {
			lastError = String.format("City %s does not exist", route);
			return false;
		}

		try {
			// findOneAndUpdate
			Bson filter = eq(Constants._ID, callSign);
			ArrayList<String> newRoute = new ArrayList<String>();
			newRoute.add(route);

			Bson updateRoute;

			if (isSingleCity) {
				updateRoute = set(Constants.ROUTE, newRoute);
			} else {
				updateRoute = addToSet(Constants.ROUTE, route);
			}
			//Relying on retryable writes here.
			planesCollection.findOneAndUpdate(filter, updateRoute);
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return false;
	}

	boolean removeFirstPlaneRoute() {

		if (isPopulated() == false) {
			return false;
		}
		
		try {
			// findOneAndUpdate
			Bson filter = eq(Constants._ID, callSign);
			Bson updateRoute = popFirst(Constants.ROUTE);
			//Relying on retryable writes here.
			planesCollection.findOneAndUpdate(filter, updateRoute);
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return false;
	}


	/**
	 * This method updates the Flight Log. NOTE: Flight Log is unending! An
	 * appropriate archival strategy has to be put in place. We use the Computed
	 * pattern to store summary information at plane level.
	 * 
	 * Also, sicne we are extending the original schema with a bunch of new fields:
	 * flightLog Array lastLandingEvent totalFlightTime totalDistanceFlown
	 * requiresMaintenance
	 * 
	 * It's a good idea to apply the Schema Versioning pattern here. We will set the
	 * Schema version to 1 here (note that if there is no schemaVersion in the doc,
	 * version is considered as 0/initial because planes can be created by backend
	 * scripts without calling our excellent APIs)
	 * 
	 * The total Distance Flown and Total Flight Time are to be made Idempotent
	 * 
	 * Hence, we do not use $inc operator, instead we calculate the values and set them upon update
	 * 
	 * @param landingEvent - most recent landing for the plane
	 * 
	 */
	boolean updateFlightLog(Document landingEvent) {

		if (isPopulated() == false) {
			return false;
		}

		try {
			//Idempotent distance - do not use $inc
			Double distanceFlown = calculateTotalDistanceFlown(totalDistanceFlown, landingEvent);

			// findOneAndUpdate
			Bson filter = eq(Constants._ID, callSign);
			Bson updates;

			Bson updateSchemaVersion = set(Constants.SCHEMA_VERSION, schemaVersion);
			Bson updateLocationEvent = push(Constants.FLIGHT_LOG, landingEvent);
			//Idempotent total flight time...do not use $inc
			Bson updateTotalFlightTime = set(Constants.TOTAL_FLIGHT_TIME,
					calculateTotalFlightTime(totalFlightTime, landingEvent)); 
			Bson updateTotalDistanceFlown = set(Constants.TOTAL_DISTANCE_FLOWN, distanceFlown);
			Bson updatePlaneRequiresMaintenance = set(Constants.REQUIRES_MAINTENANCE, true);

			/*********
			 * 
			 * Use Computed Pattern to save the total Miles and Total Flight Time
			 * 
			 * It also allows us to keep the updates IDEMPOTENT by not using $inc
			 * 
			 */
			if (planeRequiresMaintenance(distanceFlown)) {
				logger.warn(String.format("Plane %s requires maintenance", callSign));
				updates = combine(updateSchemaVersion, updateLocationEvent, updateTotalFlightTime,
						updateTotalDistanceFlown, updatePlaneRequiresMaintenance);
			} else {
				updates = combine(updateSchemaVersion, updateLocationEvent, updateTotalFlightTime,
						updateTotalDistanceFlown);
			}
			//Relying on retryable writes here.
			planesCollection.findOneAndUpdate(filter, updates);
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return false;
	}

	private ArrayList<Double> createDoubleArrayFromString(String location) {

		String values[] = location.split(",");
		ArrayList<Double> parsed = new ArrayList<Double>(values.length);
		for (int i = 0; i < values.length; i++)
			parsed.add(Double.valueOf(values[i]));
		return parsed;

	}

	private boolean planeRequiresMaintenance(Double distanceFlown) {
		if ((requiresMaintenance!=null) && (requiresMaintenance)) return true;
		if (distanceFlown != null)
			if (distanceFlown > Constants.MAINTENANCE_REQUIRED_LIMIT)
				// Probably shouldn't fly these planes anymore and throw a
				// PlaneRequiresMaintenance exception if someone tries to set a route,
				// but then, the UI will not look so cool!
				return true;
		return false;
	}

	private Double calculateTotalDistanceFlown(Double distanceFlown, Document landingEvent) {

		if ((lastLandingEvent != null) && (lastLandingEvent.get(Constants.POSITION) != null)) {
			@SuppressWarnings("unchecked")
			ArrayList<Double> lastLatLong = (ArrayList<Double>) lastLandingEvent.get(Constants.POSITION);
			@SuppressWarnings("unchecked")
			ArrayList<Double> latLong = (ArrayList<Double>) landingEvent.get(Constants.POSITION);

			if (distanceFlown == null) {
				return DistanceUtil.distance(lastLatLong.get(0), lastLatLong.get(1), latLong.get(0), latLong.get(1),
						Constants.UNIT_MILES);

			} else {
				return distanceFlown + DistanceUtil.distance(lastLatLong.get(0), lastLatLong.get(1), latLong.get(0),
						latLong.get(1), Constants.UNIT_MILES);

			}

		} else {
			/****
			 * We are computing the total miles and total flight time based solely on plane landings. 
			 * It does not include the very first flight of the plane, hence returning 0
			 */
			return 0.0;
		}

	}

	private Double calculateTotalFlightTime(Double flightTime, Document landingEvent) {

		if ((lastLandingEvent != null) && (lastLandingEvent.get(Constants.LANDING_DATE) != null)) {
			Date lastLandedDate = lastLandingEvent.getDate(Constants.LANDING_DATE);
			Date landedDate = landingEvent.getDate(Constants.LANDING_DATE);


		
			/******
			 * 
			 * Calculating the flight time difference in seconds and
			 * storing the total flight time in seconds.
			 * 
			 * Since it is a ABS operation, we lose extremely valuable milliseconds. Please do
			 * not use this for any mission critical calculations...in any case, the planes
			 * are hyper-super-duper-sonic, since they cover thousands of miless in a few seconds!
			 * 
			 ******/
			long difference_In_Seconds = Math.abs(landedDate.getTime()-lastLandedDate.getTime())/1000;
			if (flightTime == null) {
				return new Double(difference_In_Seconds);

			} else {
				return flightTime + new Double(difference_In_Seconds);

			}
		} else {
			/****
			 * We are computing the total miles and total flight time based solely on plane landings. 
			 * It does not include the very first flight of the plane, hence returning 0
			 */
			return 0.0;
		}

	}
	
	private boolean isValidCity(String landing) {
		CityDAL city = new CityDAL(mongoClient, landing);
		return city.isPopulated();
	}

	// Memeber Acessors
	String getCallSign() {
		return callSign;
	}

	ArrayList<Double> getCurrentLocation() {
		return currentLocation;
	}

	Object getHeading() {
		return heading;
	}

	ArrayList<String> getRoute() {
		return route;
	}

	String getLanded() {
		return landed;
	}

	String getStatus() {
		return status;
	}

	boolean isPopulated() {
		return populated;
	}

	String getLastError() {
		return lastError;
	}
	
	int getDocVersion() {
		return doc_version;
	}

}
