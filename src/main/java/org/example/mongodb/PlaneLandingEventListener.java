package org.example.mongodb;

import static com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

/******
 * Since this is designed to be a separate piece of code, we are using Change
 * Streams
 * 
 * This change stream monitors the planes collection for landing events. It
 * maintains a running log of all the landings that the plane has made.
 * 
 * These logs will be used to calculate total flight times and duration.
 * 
 * Please note that the complete flight log is modelled as an Array, which is
 * not inteded to be read by the application. Hence, it is imperative that
 * appropriate projections should be used while retrieving the planes, since the
 * flight log can grow to an enormous size. We have, however, kept it for
 * Analytical purposes, where the flight log can be read on a read-only replica
 * for reporting needs.
 * 
 * 
 * Subscribe to the Change Stream ONLY if the updates include "landed" field of
 * the plane else we will be bombarded with updates we are not interested in.
 ******/
public class PlaneLandingEventListener implements Runnable {

	private static final String OPERATION_TYPE = "operationType";
	private static final String UPDATE_OPERATION = "update";
	private static final String UPDATE_DESCRIPTION_UPDATED_FIELDS_LANDED = "updateDescription.updatedFields.landed";
	Logger logger;
	MongoClient mongoClient;
	private MongoCollection<Document> planesCollection;

	PlaneLandingEventListener(MongoClient mongoClient) {
		logger = LoggerFactory.getLogger(PlaneLandingEventListener.class);
		this.mongoClient = mongoClient;
	}

	boolean listenForPlaneLandings() {

		try {
			planesCollection = this.mongoClient.getDatabase(Constants.DB_NAME_LOGISTICS)
					.getCollection(Constants.COLLECTION_PLANES);

			List<Bson> pipeline;

			// Change Stream Updates where updatedFields contains landed only, otherwise we
			// get bombarded with update events
			pipeline = singletonList(
					Aggregates.match(Filters.and(Filters.exists(UPDATE_DESCRIPTION_UPDATED_FIELDS_LANDED),
							Filters.in(OPERATION_TYPE, asList(UPDATE_OPERATION)))));

			ChangeStreamIterable<Document> changes = planesCollection.watch(pipeline).fullDocument(UPDATE_LOOKUP);

			changes.forEach(new Block<ChangeStreamDocument<Document>>() {
				@Override
				public void apply(ChangeStreamDocument<Document> t) {

					CityDAL city = new CityDAL(mongoClient, t.getFullDocument().getString(Constants.LANDED));

					Document landedEvent = new Document()
							.append(Constants.LANDING_LOCATION, t.getFullDocument().getString(Constants.LANDED))
							.append(Constants.LANDING_DATE, new Date()).append(Constants.POSITION, city.getPosition());
					PlaneDAL plane;

					plane = new PlaneDAL(mongoClient, t.getFullDocument().getString(Constants._ID));
					plane.updateFlightLog(landedEvent);

				}
			});
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		return true; //Run forever

	}

	@Override
	public void run() {
		logger.info("Thread {} has started.");

		while (true) {
			if (listenForPlaneLandings() == false) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}

}
