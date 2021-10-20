package org.example.mongodb;

import static spark.Spark.after;
import static spark.Spark.delete;
import static spark.Spark.externalStaticFileLocation;
import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;
import static spark.Spark.put;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class WebService {
	static final String version = "0.0.1";
	static Logger logger;
	private static String static_dir;

	public static int ordinalIndexOf(String str, String substr, int n) {
		int pos = -1;
		do {
			pos = str.indexOf(substr, pos + 1);
		} while (n-- > 0 && pos != -1);
		return pos;
	}

	public static void main(String[] args) {
		port(5000);
		static_dir = System.getProperty("user.dir");
		static_dir = static_dir.substring(0,ordinalIndexOf(static_dir,"/",2)) + "/static";
		externalStaticFileLocation(static_dir);
		
		LogManager.getLogManager().reset();
		
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();

		logger = LoggerFactory.getLogger(WebService.class);
		logger.info(version);

	    //Note: Needs to be a replica set for Change Stream processing
		String URI="mongodb://localhost:27017";
        if(args.length > 0)
        {
                URI = args[0];
                
        }
        
        MongoClient mongoClient = new MongoClient(new MongoClientURI(URI));
 
		APIRoutes apiRoutes = new APIRoutes(mongoClient);
			// *** PLANES ***
				//Fetch planes
				// E.G. curl -X GET http://localhost:5000/planes
				get("/planes",(req,res) -> apiRoutes.getPlanes(req,res));

				//Fetch plane by ID
				// E.G. curl -X GET http://localhost:5000/planes/CARGO10
				get("/planes/*",(req,res) -> apiRoutes.getPlaneById(req,res));

				// Update location, heading, and landed for a plane
				// E.G. curl -X PUT http://localhost:5000/planes/CARGO10/location/2,3/240/London
				put("/planes/*/location/*/*/*",(req,res) -> apiRoutes.updatePlaneLocationAndLanding(req,res));

				//Update location and heading for a plane
				// E.G. curl -X PUT http://localhost:5000/planes/CARGO10/location/2,3/240
				put("/planes/*/location/*/*",(req,res) -> apiRoutes.updatePlaneLocation(req,res));

				//Replace a Plane's Route with a single city
				// E.G. curl -X PUT http://localhost:5000/planes/CARGO10/route/London
				put("/planes/*/route/*",(req,res) -> apiRoutes.addPlaneRoute(req,res,true));

				//Add a city to a Plane's Route
				// E.G. curl -X POST http://localhost:5000/planes/CARGO10/route/London
				post("/planes/*/route/*",(req,res) -> apiRoutes.addPlaneRoute(req,res,false));

				//Remove the first entry in the list of a Planes route
				// E.G. curl -X DELETE http://localhost:5000/planes/CARGO10/route/destination
				delete("/planes/*/route/destination",(req,res) -> apiRoutes.removeFirstPlaneRoute(req,res));

			// ************

			
			// *** CITIES ***
				//Fetch ALL cities
				// E.G. curl -X GET http://localhost:5000/cities
				get("/cities",(req,res) -> apiRoutes.getCities(req,res));
				
				//Fetch City Neighbors by ID
				// E.G. curl -X GET http://localhost:5000/cities/London/neighbors/5
				get("/cities/*/neighbors/*",(req,res) -> apiRoutes.getCityNeighbors(req,res));
			
				//Fetch City by ID
				// E.G. curl -X GET http://localhost:5000/cities/London
				get("/cities/*",(req,res) -> apiRoutes.getCityById(req,res));
				

			// ************

			
			// *** CARGO ***
			// ************
				//Fetch Cargo by ID
				// E.G. curl -X GET http://localhost:5000/cargo/location/London
				get("/cargo/location/*",(req,res) -> apiRoutes.getCargoAtLocation(req,res));

				// Create a new cargo at "location" which needs to get to "destination" - error if neither location nor destination exist as cities. Set status to "in progress" 
				// E.G. curl -X POST http://localhost:5000/cargo/London/to/Cairo
				post("/cargo/*/to/*",(req,res) -> apiRoutes.createCargo(req,res));

				// Set status field to 'Delivered' - Increment some count of delivered items too.
				// E.G. curl -X PUT http://localhost:5000/cargo/5f45303156fd8ce208650caf/delivered
				put("/cargo/*/delivered",(req,res) -> apiRoutes.cargoDelivered(req,res));

				// Mark that the next time the courier (plane) arrives at the location of this package it should be onloaded by setting the courier field - courier should be a plane.
				// E.G. curl -X PUT http://localhost:5000/cargo/5f45303156fd8ce208650caf/courier/CARGO10
				put("/cargo/*/courier/*",(req,res) -> apiRoutes.cargoAssignCourier(req,res));

				// Unset the value of courier on a given piece of cargo
				// E.G. curl -X DELETE http://localhost:5000/cargo/5f4530d756fd8ce208650d83/courier
				delete("/cargo/*/courier",(req,res) -> apiRoutes.cargoUnsetCourier(req,res));

				// Move a piece of cargo from one location to another (plane to city or vice-versa)
				// E.G. curl -X PUT http://localhost:5000/cargo/5f4530d756fd8ce208650d83/location/London
				put("/cargo/*/location/*",(req,res) -> apiRoutes.cargoMove(req,res));

			after((req, res) -> {
				res.type("application/json");
			});
		
			//Start the Task 3(a) listener here
			startPlaneChangeListener(mongoClient);


		return;
	}
	
	/**
	 * Starts the Change Stream Listener for Plane Landing Events
	 * 
	 * @param mongoClient - the MongoClient to use
	 */
	private static void startPlaneChangeListener(MongoClient mongoClient) {
		
		//Listen to the change stream on this thread
		ExecutorService simexec = Executors.newSingleThreadExecutor();

		simexec.execute(new PlaneLandingEventListener(mongoClient));
		
		simexec.shutdown();
	}

}
