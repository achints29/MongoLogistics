package org.example.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.near;

import java.util.ArrayList;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoQueryException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;

public class CityDAL {

	Logger logger;
	private static final double DONT_INCLUDE_THE_CURRENT_CITY = 1.0; // min distance
	private MongoClient mongoClient;
	private MongoCollection<Document> citiesCollection;

	private boolean populated;
	private String lastError;
	private String name;
	private String country;
	private ArrayList<Double> location;

	public CityDAL(MongoClient mongoClient) {
		logger = LoggerFactory.getLogger(CityDAL.class);
		this.mongoClient = mongoClient;
		citiesCollection = this.mongoClient.getDatabase(Constants.DB_NAME_LOGISTICS)
				.getCollection(Constants.COLLECTION_CITIES);

		lastError = "";
	}

	public CityDAL(MongoClient mongoClient, String cityId) {
		this(mongoClient);
		Document dbdata = citiesCollection.find(eq(Constants._ID, cityId)).first();
		if (dbdata != null) {
			parseDocument(dbdata);
		} else {
			lastError = String.format("City %s does not exist", cityId);
		}
	}

	@SuppressWarnings("unchecked")
	private void parseDocument(Document doc) {
		populated = false;
		try {
			name = doc.getString(Constants._ID);
			country = doc.getString(Constants.COUNTRY);
			location = (ArrayList<Double>) doc.get(Constants.POSITION);
		} catch (Exception e) {
			lastError = e.getMessage();
			populated = false;
			return;
		}
		populated = true;
	}

	ArrayList<String> getAllCities() {
		ArrayList<String> allCities = new ArrayList<String>();

		try {
			MongoCursor<Document> resultsIterator = null;
			FindIterable<Document> results = citiesCollection.find();
			for (resultsIterator = results.iterator(); resultsIterator.hasNext();) {
				Document toReturn = createCityResponse(resultsIterator.next());
				allCities.add(toReturn.toJson());
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return allCities;
	}

	/**
	 * Returns neighbors using Geo Near query Needs a 2dsphere index created on
	 * position field to work
	 * 
	 * @param limit - count of returned cities
	 * @return empty, in case of any exceptions
	 */
	ArrayList<Document> getNeighbors(String limit) {

		ArrayList<Document> neighboringCities = new ArrayList<Document>();
		if (isPopulated() == false) {
			return neighboringCities;
		}

		try {
			MongoCursor<Document> resultsIterator = null;
			Bson filter = near(Constants.POSITION, new Point(new Position(location)), Double.MAX_VALUE,
					DONT_INCLUDE_THE_CURRENT_CITY);
			FindIterable<Document> results = citiesCollection.find(filter).limit(Integer.parseInt(limit));

			for (resultsIterator = results.iterator(); resultsIterator.hasNext();) {
				Document toReturn = createCityResponse(resultsIterator.next());
				neighboringCities.add(toReturn);
			}

		} catch (NumberFormatException e) {
			logger.warn("limit needs to be an integer");
			lastError = e.getMessage();
		} catch (MongoQueryException e) {
			logger.error("Possibly, the 2dsphere index has not been created on the 'position' field.  Please do so!");
			lastError = e.getMessage();
		} catch (Exception e) {
			logger.error(e.getMessage());
			lastError = e.getMessage();
		}
		return neighboringCities;
	}

	private Document createCityResponse(Document document) {
		Document toReturn = new Document();
		toReturn.append(Constants.NAME, document.getString(Constants._ID));
		toReturn.append(Constants.COUNTRY, document.getString(Constants.COUNTRY));
		toReturn.append(Constants.LOCATION, document.get(Constants.POSITION));
		return toReturn;
	}

	
	// Memeber Acessors
	boolean isPopulated() {
		return populated;
	}

	String getLastError() {
		return lastError;
	}

	Object getName() {
		return name;
	}

	Object getCountry() {
		return country;
	}

	Object getPosition() {
		return location;
	}
}
