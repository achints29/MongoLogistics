package org.example.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.unset;

import java.util.ArrayList;
import java.util.Date;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class CargoDAL {

	Logger logger;
	private MongoClient mongoClient;
	private MongoCollection<Document> cargoCollection;

	private boolean populated;
	private String lastError;

	private String id;
	private String destination;
	private String location;
	private Date received;
	private String status;
	private Date delivered;
	private String courier;

	public CargoDAL(MongoClient mongoClient) {
		logger = LoggerFactory.getLogger(CargoDAL.class);
		this.mongoClient = mongoClient;
		cargoCollection = this.mongoClient.getDatabase(Constants.DB_NAME_LOGISTICS)
				.getCollection(Constants.COLLECTION_CARGO)
				.withWriteConcern(WriteConcern.MAJORITY)
				.withReadConcern(ReadConcern.MAJORITY);
		lastError = "";
	}

	public CargoDAL(MongoClient mongoClient, String cargoId) {
		this(mongoClient);
		Document dbdata = cargoCollection.find(eq(Constants._ID, new ObjectId(cargoId))).first();
		if (dbdata != null) {
			parseDocument(dbdata);
		} else {
			lastError = String.format("Cargo %s does not exist", cargoId);
		}
	}

	private void parseDocument(Document doc) {
		populated = false;
		try {
			id = doc.getObjectId(Constants._ID).toString();
			destination = doc.getString(Constants.DESTINATION);
			location = doc.getString(Constants.LOCATION);
			received = doc.getDate(Constants.RECEIVED);
			courier = doc.getString(Constants.COURIER);
			delivered = doc.getDate(Constants.DELIVERED);
			status = doc.getString(Constants.STATUS);
		} catch (Exception e) {
			lastError = e.getMessage();
			populated = false;
			return;
		}
		populated = true;
	}

	boolean createCargo(String location, String destination) {
		
		if(!isValidCity(location)) {
			lastError = String.format("City %s does not exist", location);
			return false;
		}
		
		if(!isValidCity(destination)) {
			lastError = String.format("City %s does not exist", destination);
			return false;
		}
		
		try {
			Document cargo = new Document();
			cargo.append(Constants.LOCATION, location);
			cargo.append(Constants.DESTINATION, destination);
			cargo.append(Constants.RECEIVED, new Date());
			cargo.append(Constants.STATUS, Constants.STATUS_INPROCESS);
			//Relying on retryable writes here.
			cargoCollection.insertOne(cargo);

		} catch (Exception e) {
			lastError = e.getMessage();
			return false;
		}
		lastError = "";
		return true;
	}
	/*
	 * Gets all "in process" cargo at a given location.
	 * 
	 * Ensure that there is an index on location + status for performance
	 * 
	 */
	ArrayList<String> cargoAtLocation(String atLocation) {
		
		ArrayList<String> allCargo = new ArrayList<String>();
		if(!isValidCityOrPlane(atLocation)) {
			logger.error(String.format("Location %s does not exist", atLocation));
			lastError = String.format("Location %s does not exist", atLocation);
			return allCargo;
		}
		
		MongoCursor<Document> resultsIterator = null;
		Bson locationFilter = eq(Constants.LOCATION, atLocation);
		Bson statusFilter = eq(Constants.STATUS, Constants.STATUS_INPROCESS);

		FindIterable<Document> results = cargoCollection.find(and(locationFilter, statusFilter));
	
		for (resultsIterator = results.iterator(); resultsIterator.hasNext();) {
			Document document = resultsIterator.next();
			Document toReturn = new Document();
			toReturn.append(Constants.CARGO_ID, document.getObjectId(Constants._ID).toString());
			toReturn.append(Constants.LOCATION, document.get(Constants.LOCATION));
			toReturn.append(Constants.DESTINATION, document.getString(Constants.DESTINATION));
			toReturn.append(Constants.COURIER, document.getString(Constants.COURIER));
			toReturn.append(Constants.RECEIVED, document.getDate(Constants.RECEIVED).toString());
			if (document.getDate(Constants.DELIVERED) != null)
				toReturn.append(Constants.DELIVERED, document.getDate(Constants.DELIVERED).toString());
			toReturn.append(Constants.STATUS, document.getString(Constants.STATUS));
			allCargo.add(toReturn.toJson());
		}
		return allCargo;
	}

	boolean markDelivered() {

		if (isPopulated() == false) {
			return false;
		}

		try {
			// findOneAndUpdate
			Bson filter = eq(Constants._ID, new ObjectId(id));

			Bson updateStatus = set(Constants.STATUS, Constants.STATUS_DELIVERED);
			Bson updateDelivery = set(Constants.DELIVERED, new Date());
			Bson updates = combine(updateStatus, updateDelivery);
			//Relying on retryable writes here.
			cargoCollection.findOneAndUpdate(filter, updates);
			lastError = "";
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return false;

	}

	boolean assignCourier(String courier) {

		if (isPopulated() == false) {
			return false;
		}
		
		if(!isValidPlane(courier)) {
			lastError = String.format("Plane %s does not exist", courier);
			return false;
		}

		try {
			// findOneAndUpdate
			Bson filter = eq(Constants._ID, new ObjectId(id));

			Bson updateCargo = set(Constants.COURIER, courier);
			//Relying on retryable writes here.
			cargoCollection.findOneAndUpdate(filter, updateCargo);
			lastError = "";
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return false;

	}

	boolean unsetCourier() {

		if (isPopulated() == false) {
			return false;
		}

		try {
			// findOneAndUpdate
			Bson filter = eq(Constants._ID, new ObjectId(id));

			Bson unsetCourier = unset(Constants.COURIER);

			//Relying on retryable writes here.
			cargoCollection.findOneAndUpdate(filter, unsetCourier);
			lastError = "";
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return false;

	}

	boolean moveCargo(String newLocation) {

		if (isPopulated() == false) {
			return false;
		}
		
		if(!isValidCityOrPlane(newLocation)) {
			logger.error(String.format("Location %s does not exist", newLocation));
			lastError = String.format("Location %s does not exist", newLocation);
			return false;
		}

		try {
			// findOneAndUpdate
			Bson filter = eq(Constants._ID, new ObjectId(id));

			Bson updateLocation = set(Constants.LOCATION, newLocation);
			
			//Relying on retryable writes here.
			cargoCollection.findOneAndUpdate(filter, updateLocation);
			lastError = "";
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return false;

	}
	

	private boolean isValidCity(String location) {
		CityDAL city = new CityDAL(mongoClient, location);
		return city.isPopulated();
	}
	
	private boolean isValidPlane(String location) {
		PlaneDAL plane = new PlaneDAL(mongoClient, location);
		return plane.isPopulated();
	}
	
	private boolean isValidCityOrPlane(String location) {
		return isValidCity(location)||isValidPlane(location);
	}

	// Memeber Acessors
	String getId() {
		return id;
	}

	String getDestination() {
		return destination;
	}

	String getCourier() {
		return courier;
	}

	String getLocation() {
		return location;
	}

	Date getReceived() {
		return received;
	}

	Date getDelivered() {
		return delivered;
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
}