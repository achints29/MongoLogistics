package org.example.mongodb;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;

import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;

import spark.Request;
import spark.Response;

public class APIRoutes {
	Logger logger;
	MongoClient mongoClient;

	// Define how to write JSON with types like Binary, GUID , Decimal128 and Date()
	// output as strings by default MongoDB preserves this type information
	// outputting JSON.
	// We could use a standard JSON writer like GSON but MDB comes with one.

	JsonWriterSettings plainJSON = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED)
			.binaryConverter((value, writer) -> writer.writeString(Base64.getEncoder().encodeToString(value.getData())))
			.dateTimeConverter((value, writer) -> {
				ZonedDateTime zonedDateTime = Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC);
				writer.writeString(DateTimeFormatter.ISO_DATE_TIME.format(zonedDateTime));
			}).decimal128Converter((value, writer) -> writer.writeString(value.toString()))
			.objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
			.symbolConverter((value, writer) -> writer.writeString(value)).build();

	// So we can connect any DALs we use to the database
	APIRoutes(MongoClient mongoClient) {
		logger = LoggerFactory.getLogger(APIRoutes.class);
		this.mongoClient = mongoClient;
		// Force a connection test - will error out if it cannot
		mongoClient.getDatabase("any").runCommand(new Document("ping", 1));
	}

	// *** PLANES ***

	// Fetch planes
	// E.G. curl -X GET http://localhost:5000/planes
	public ArrayList<String> getPlanes(Request req, Response res) {

		PlaneDAL planes = new PlaneDAL(mongoClient);

		return planes.getAllPlanes();
	}

	// Fetch plane by ID
	// E.G. curl -X GET http://localhost:5000/planes/CARGO10
	public String getPlaneById(Request req, Response res) {
		String planeId = req.splat()[0];
		PlaneDAL plane;

		plane = new PlaneDAL(mongoClient, planeId);

		if (plane.isPopulated() == false) {
			res.status(404);
			return new Document("ok", false).append("error", plane.getLastError()).toJson();
		}
		// Construct a JSON Document to return in our API
		Document planeForAPI = new Document();
		planeForAPI.append(Constants.CALLSIGN, plane.getCallSign());
		planeForAPI.append(Constants.CURRENT_LOCATION, plane.getCurrentLocation());
		planeForAPI.append(Constants.HEADING, plane.getHeading());
		planeForAPI.append(Constants.ROUTE, plane.getRoute());
		planeForAPI.append(Constants.LANDED, plane.getLanded());
		
		//Application doesn't need the additional fields for Schema Version 1..API won't expose them here

		return planeForAPI.toJson(plainJSON);
	}

	// Update location, heading, and landed for a plane
	// E.G. curl -X PUT http://localhost:5000/planes/CARGO10/location/2,3/240/London
	public String updatePlaneLocationAndLanding(Request req, Response res) {
		String planeId = req.splat()[0];
		String location = req.splat()[1];
		String heading = req.splat()[2];
		String landing = req.splat()[3];

		PlaneDAL plane;

		plane = new PlaneDAL(mongoClient, planeId);

		if (plane.isPopulated() == false) {
			res.status(404);
			return new Document("ok", false).append("error", plane.getLastError()).toJson();
		}

		if (plane.updateLocationHeadingLanding(location, heading, landing) == false) {
			res.status(404);
			return new Document("ok", false).append("error", plane.getLastError()).toJson();
		}
		return new Document("ok", true).toJson();

	}

	// Update location and heading for a plane
	// E.G. curl -X PUT http://localhost:5000/planes/CARGO10/location/2,3/240
	public String updatePlaneLocation(Request req, Response res) {
		String planeId = req.splat()[0];
		String location = req.splat()[1];
		String heading = req.splat()[2];

		PlaneDAL plane;

		plane = new PlaneDAL(mongoClient, planeId);

		if (plane.isPopulated() == false) {
			res.status(404);
			return new Document("ok", false).append("error", plane.getLastError()).toJson();
		}

		if (plane.updateLocationHeading(location, heading) == false) {
			res.status(404);
			return new Document("ok", false).append("error", plane.getLastError()).toJson();
		}
		return new Document("ok", true).toJson();
	}

	// Replace a Plane's Route with a single city
	// E.G. curl -X PUT http://localhost:5000/planes/CARGO10/route/London
	public String addPlaneRoute(Request req, Response res, Boolean isSingleCity) {
		String planeId = req.splat()[0];
		String route = req.splat()[1];

		PlaneDAL plane;

		plane = new PlaneDAL(mongoClient, planeId);


		if (plane.isPopulated() == false) {
			res.status(404);
			return new Document("ok", false).append("error", plane.getLastError()).toJson();
		}
		
		// Probably shouldn't allow to fly planes requiring maintenance and throw a
		// PlaneRequiresMaintenance exception if someone tries to set a route,
		// but then, the UI will not look so cool! 
		// Right now, we let planes requiring maintenance to still fly :)
		if (plane.addPlaneRoute(route, isSingleCity) == false) {
			res.status(404);
			return new Document("ok", false).append("error", plane.getLastError()).toJson();
		}
		return new Document("ok", true).toJson();
	}

	// Remove the first entry in the list of a Planes route
	// E.G. curl -X DELETE http://localhost:5000/planes/CARGO10/route/destination
	public String removeFirstPlaneRoute(Request req, Response res) {
		String planeId = req.splat()[0];

		PlaneDAL plane;

		plane = new PlaneDAL(mongoClient, planeId);

		if (plane.isPopulated() == false) {
			res.status(404);
			return new Document("ok", false).append("error", plane.getLastError()).toJson();
		}

		if (plane.removeFirstPlaneRoute() == false) {
			res.status(404);
			return new Document("ok", false).append("error", plane.getLastError()).toJson();
		}
		return new Document("ok", true).toJson();
	}

	// ************

	// *** CITIES ***

	// Fetch ALL cities
	// E.G. curl -X GET http://localhost:5000/cities
	public ArrayList<String> getCities(Request req, Response res) {
		CityDAL cities = new CityDAL(mongoClient);

		return cities.getAllCities();
	}

	// Fetch City Neighbors by ID
	// E.G. curl -X GET http://localhost:5000/cities/London/neighbors/5
	public String getCityNeighbors(Request req, Response res) {
		String cityId = req.splat()[0];
		String limit = req.splat()[1];

		CityDAL city;
		ArrayList<Document> neighbors = new ArrayList<Document>();

		city = new CityDAL(mongoClient, cityId);

		if (city.isPopulated() == false) {
			res.status(404);
			return new Document("ok", false).append("error", city.getLastError()).append("neighbors", neighbors).toJson();
		}
		neighbors = city.getNeighbors(limit);
		Document toReturn = new Document().append("neighbors", neighbors);

		return toReturn.toJson(plainJSON);
	}

	// Fetch City by ID
	// E.G. curl -X GET http://localhost:5000/cities/London
	public String getCityById(Request req, Response res) {
		String cityId = req.splat()[0];

		CityDAL city;

		city = new CityDAL(mongoClient, cityId);

		if (city.isPopulated() == false) {
			res.status(404);
			return new Document("ok", false).append("error", city.getLastError()).toJson();
		}
		// Construct a JSON Document to return in our API
		Document cityForAPI = new Document();
		cityForAPI.append("name", city.getName());
		cityForAPI.append("country", city.getCountry());
		cityForAPI.append("location", city.getPosition());

		return cityForAPI.toJson(plainJSON);

	}

	// ************

	// *** CARGO ***
	// ************

	// Fetch Cargo by ID
	// E.G. curl -X GET http://localhost:5000/cargo/location/London
	public ArrayList<String> getCargoAtLocation(Request req, Response res) {
		String location = req.splat()[0];

		CargoDAL cargo;

		cargo = new CargoDAL(mongoClient);
		return cargo.cargoAtLocation(location);

	}

	// Create a new cargo at "location" which needs to get to "destination" - error
	// if neither location nor destination exist as cities. Set status to "in
	// progress"
	// E.G. curl -X POST http://localhost:5000/cargo/London/to/Cairo
	public String createCargo(Request req, Response res) {
		String location = req.splat()[0];
		String destination = req.splat()[1];

		CargoDAL cargo;

		cargo = new CargoDAL(mongoClient);
		if (cargo.createCargo(location, destination)) {
			return new Document("ok", true).toJson();
		}else {
			res.status(404);
			return new Document("ok", false).append("error", cargo.getLastError()).toJson();
		}

	}

	// Set status field to 'Delivered' - Increment some count of delivered items
	// too.
	// E.G. curl -X PUT
	// http://localhost:5000/cargo/5f45303156fd8ce208650caf/delivered
	public String cargoDelivered(Request req, Response res) {
		String cargoId = req.splat()[0];
		CargoDAL cargo;

		cargo = new CargoDAL(mongoClient, cargoId);
		if (cargo.isPopulated() == false) {
			res.status(404);
			return new Document("ok", false).append("error", cargo.getLastError()).toJson();
		}

		if (cargo.markDelivered()) {
			return new Document("ok", true).toJson();
		} else {
			res.status(404);
			return new Document("ok", false).append("error", cargo.getLastError()).toJson();
		}

	}

	// Mark that the next time the courier (plane) arrives at the location of this
	// package it should be onloaded by setting the courier field - courier should
	// be a plane.
	// E.G. curl -X PUT
	// http://localhost:5000/cargo/5f45303156fd8ce208650caf/courier/CARGO10
	public String cargoAssignCourier(Request req, Response res) {
		String cargoId = req.splat()[0];
		String courier = req.splat()[1];
		CargoDAL cargo;

		cargo = new CargoDAL(mongoClient, cargoId);
		if (cargo.isPopulated() == false) {
			res.status(404);
			return new Document("ok", false).append("error", cargo.getLastError()).toJson();
		}
	
		if (cargo.assignCourier(courier)) {
			return new Document("ok", true).toJson();
		} else {
			res.status(404);
			return new Document("ok", false).append("error", cargo.getLastError()).toJson();
		}
	}

	// Unset the value of courier on a given piece of cargo
	// E.G. curl -X DELETE
	// http://localhost:5000/cargo/5f4530d756fd8ce208650d83/courier
	public String cargoUnsetCourier(Request req, Response res) {
		String cargoId = req.splat()[0];
		CargoDAL cargo;

		cargo = new CargoDAL(mongoClient, cargoId);
		if (cargo.isPopulated() == false) {
			res.status(404);
			return new Document("ok", false).append("error", cargo.getLastError()).toJson();
		}
		
		if (cargo.unsetCourier()) {
			return new Document("ok", true).toJson();
		} else {
			res.status(404);
			return new Document("ok", false).append("error", cargo.getLastError()).toJson();
		}
	}

	// Move a piece of cargo from one location to another (plane to city or
	// vice-versa)
	// E.G. curl -X PUT
	// http://localhost:5000/cargo/5f4530d756fd8ce208650d83/location/London
	public String cargoMove(Request req, Response res) {
		String cargoId = req.splat()[0];
		String location = req.splat()[1];
		CargoDAL cargo;

		cargo = new CargoDAL(mongoClient, cargoId);
		if (cargo.isPopulated() == false) {
			res.status(404);
			return new Document("ok", false).append("error", cargo.getLastError()).toJson();
		}
		
		if (cargo.moveCargo(location)) {
			return new Document("ok", true).toJson();
		} else {
			res.status(404);
			return new Document("ok", false).append("error", cargo.getLastError()).toJson();
		}
	}

}
