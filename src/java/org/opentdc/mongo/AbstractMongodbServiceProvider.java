/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Arbalo AG
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.opentdc.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.ServletContext;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.opentdc.service.exception.InternalServerErrorException;
import org.opentdc.service.exception.NotFoundException;
import org.opentdc.service.exception.ValidationException;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import static com.mongodb.client.model.Filters.*;


/**
 * Abstract implementation of mongodb access.
 * Use it as follows:  connect(); getCollection(name); your operations; disconnect().
 * The data must be converted into a org.bson.Document first.
 * @author Bruno Kaiser
 *
 * @param <T>
 */
public class AbstractMongodbServiceProvider<T> {
	private static final Logger logger = Logger.getLogger(AbstractMongodbServiceProvider.class.getName());
	private static String mongodbHost = null;
	private static int mongodbPort = 27017;		// default mongodb port
	private static String mongodbName = null;
	private static String mongodbUser = null;
	private static String mongodbPwd = null;  		// Password of dbUser in mongoDB
	private static MongoClient mongoClient = null;  // represents a pool of connections for a database cluster
	protected MongoCollection<Document> collection = null; // corresponds to a table
	protected String collectionName = null;
	
	/**
	 * Constructor
	 * @param context  the servlet context with configuration attributes
	 */
	public AbstractMongodbServiceProvider(
		ServletContext context) 
	{
		// TODO: support multiple ServerAddresses for replica sets  host1;host2;host3
		mongodbHost = context.getInitParameter("mongodbHost"); 
		if (mongodbHost == null || mongodbHost.isEmpty()) {
			mongodbHost = "localhost";		// only for test environment
		}
		// TODO: support multiple ports for replica sets    port1;port2;port3
		String _buf = context.getInitParameter("mongodbPort");
		if (_buf != null && !_buf.isEmpty()) {
			mongodbPort = new Integer(_buf).intValue();
		}
		String _dbName = context.getInitParameter("mongodbName");
		if (_dbName == null || _dbName.isEmpty()) {
			mongodbName = "opentdc_v1";
		}
		else {
			mongodbName = _dbName;
		}
		mongodbUser = context.getInitParameter("mongodbUser");	// may be null !
		mongodbPwd = context.getInitParameter("mongodbPwd");	// may be null !
		logger.info("AbstractMongodbServiceProvider: mongodbHost=" + mongodbHost + 
				", mongodbPort=" + mongodbPort + 
				", mongodbName=" + mongodbName +
				", mongodbUser=" + mongodbUser);
	}
	
	/**
	 * Establish a connection to the MongoDB database.
	 * @throws InternalServerErrorException if the connection could not be established
	 */
	protected static void connect()
			throws InternalServerErrorException 
	{
		ServerAddress _serverAddress = new ServerAddress(mongodbHost, mongodbPort);
		if (mongoClient != null) {
			logger.warning("re-connecting to an already open mongoDB connection");
		} else {
			try {
				if (mongodbUser == null || mongodbUser.isEmpty() || 
						mongodbPwd == null || mongodbPwd.isEmpty()) {
					logger.warning("access to MongoDB is insecure");
					mongoClient = new MongoClient(_serverAddress);
				}
				else {
					MongoCredential _credential = 
							MongoCredential.createCredential(mongodbUser, mongodbName, mongodbPwd.toCharArray());
					mongoClient = 
							new MongoClient(_serverAddress, Arrays.asList(_credential));		
				}
				// default WriteConcern.ACKNOWLEDGED
				// optionally, choose another WriteConcern eg. _mongoClient.setWriteConcern(WriteConcern.JOURNALED);
			}
			catch (MongoException _ex) {
				logger.info(_ex.getMessage());
				logger.info(_ex.getStackTrace().toString());		
				throw new InternalServerErrorException("could not establish a connection to the mongodb: MongoException in AbstractMongodbServiceProvider.constructor");
			}
		}
	}
	
	/**
	 * Set and return the collection with name collectionName. In MongoDB, a collection
	 * corresponds to a table in a relational database.
	 * @param collectionName the name of the collection to retrieve.
	 * @return a collection of Documents
	 * @throws InternalServerErrorException if there is a problem getting the collection
	 * @throws ValidationException if the collectionName is not correct
	 */
	protected MongoCollection<Document> getCollection(
			String collectionName) 
			throws InternalServerErrorException, ValidationException 
	{
		if (collectionName == null ||	collectionName.isEmpty()) {
			throw new ValidationException("collectionName ist not set");
		}
		if (mongoClient == null) {
			throw new InternalServerErrorException("trying to get collection <" + collectionName + "> from non-existing DB; please connect() first.");
		}
		try {
			MongoDatabase _db = mongoClient.getDatabase(mongodbName);
			collection = _db.getCollection(collectionName);
			this.collectionName = collectionName;
		}
		catch (Exception _ex) {
			logger.info(_ex.getMessage());
			logger.info(_ex.getStackTrace().toString());		
			throw new InternalServerErrorException("could not get collection <" + collectionName + ">.");			
		}
		return collection;
	}
	
	/**
	 * Return a list of Documents. In MongoDB, a Document corresponds to a row in a relational DB.
	 * @param position the element to start the return set with
	 * @param size the amount of documents to return
	 * @return a list of Documents (ie database objects)
	 */
	protected List<Document> list(int position, int size) {
		// TODO: queries are not yet supported
		FindIterable<Document> _iterableResult = 
				(size == 0)  ? 
						collection.find().skip(position) : 
						collection.find().skip(position).limit(size);
		List<Document> _resultList = new ArrayList<Document>();
		for (Document _doc : _iterableResult) {
			_resultList.add(_doc);
		}
		logger.info("list(queryObject, " + position + ", " + size + ") -> " + _resultList.size() + " objects.");
		return _resultList;
	}
	
	/**
	 * Create a new database object (aka Document). 
	 * The ID should not be set as it is generated by the MongoDB.
	 * @param document the object to insert into the database.
	 */
	protected void create(Document document) {
		try {
			collection.insertOne(document);	
			logger.info("create(" + document + ") -> OK");
		}
		catch (Exception _ex) {
			logger.info(_ex.getMessage());
			logger.info(_ex.getStackTrace().toString());		
			throw new InternalServerErrorException("could not create a dbobject");			
		}
	}
	
	/**
	 * Retrieve one single object based on its ID.
	 * @param id the unique identificator of the object
	 * @return the object found or null if there was no such document
	 */
	protected Document readOne(
			String id) 
	{
		logger.info("readOne(" + id + ")");
		Document _doc = null;
		try {
			_doc = collection.find(eq("_id", new ObjectId(id))).first();
		}
		catch (Exception _ex) {
			logger.warning(_ex.getMessage());
			logger.warning(_ex.getStackTrace().toString());		
			// logging the error, returning with null (ie not found)
			// typically this is IllegalArgumentException: invalid hexadecimal representation of an ObjectId
		}
		return _doc;
	}
	
	/**
	 * Update the database object with a given unique id.
	 * @param id the unique identificator of the object to update
	 * @param document the new data of the database object
	 * @return the updated object containing the new data
	 * @throws NotFoundException if no object with the given id was found
	 * @throws InternalServerErrorException if there was a problem getting the object from mongodb
	 */
	protected void update(
			String id, 
			Document document) 
			throws NotFoundException, InternalServerErrorException
	{
		UpdateResult _result = null;
		try {
			logger.info("update(" + id + ", " + document.toJson() + ")");
			_result = collection.updateOne(eq("_id", new ObjectId(id)), new Document("$set", document));
			// TODO: better handling of UpdateResult
		}
		catch (Exception _ex) {
			logger.info(_ex.getMessage());
			logger.info(_ex.getStackTrace().toString());		
			throw new InternalServerErrorException("could not update dbobject <" + id + ">");			
		}
		if (_result.getModifiedCount() == 0) {
			throw new NotFoundException("dbobject with id <" + id + "> was not found in collection " + collectionName + ".");
		}
		logger.info("update(" + id + ", " + document.toJson() + ") -> OK");
	}
	
	/**
	 * Delete the database object with a given unique id.
	 * @param id the unique identificator of the object to delete
	 * @throws NotFoundException if no object with the given id is found
	 */
	protected void deleteOne(
			String id) 
		throws NotFoundException
	{
		DeleteResult _result = collection.deleteOne(eq("_id", new ObjectId(id)));
		if (_result.getDeletedCount() == 0) {
			throw new NotFoundException("dbobject with id <" + id + "> was not found in collection " + collectionName + ".");
		}
		logger.info("deleteOne(" + id + ") -> OK");
	}
	
	/**
	 * Disconnect the MongoDB.
	 */
	protected static void disconnect()
	{
		if (mongoClient == null) {
			logger.warning("trying to close a non-existing mongo-connection");
		} else {
			mongoClient.close();
		}
		logger.info("disconnect() -> OK");
	}
}
