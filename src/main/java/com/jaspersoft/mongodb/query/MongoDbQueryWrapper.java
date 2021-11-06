package com.jaspersoft.mongodb.query;

import com.jaspersoft.mongodb.connection.MongoDbConnection;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import net.sf.jasperreports.engine.JRException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class MongoDbQueryWrapper {
    private static final Log logger = LogFactory.getLog(MongoDbQueryWrapper.class);
    public static final String FIND_QUERY_KEY = "findQuery";

    public static final String FIND_QUERY_REGEXP_KEY = "findQueryRegEx";
    public static final String FIND_FIELDS_KEY = "findFields";
    public static final String SORT_KEY = "sort";
    public static final String LIMIT_KEY = "limit";
    public static final String COLLECTION_NAME_KEY = "collectionName";
    public static final String ROWS_TO_PROCESS_KEY = "rowsToProcess";
    public static final String MAP_REDUCE_KEY = "mapReduce";
    public static final String MAP_KEY = "map";
    public static final String MAPQUERY_KEY = "query";
    public static final String REDUCE_KEY = "reduce";
    public static final String OUT_KEY = "out";
    private static final String OUT_DB_KEY = "db";
    private static final String FINALIZE_KEY = "finalize";
    private static final String RUN_COMMAND_KEY = "runCommand";
    private static final String RUN_AGGREGATE_KEY = "aggregate";
    private static final String RUN_AGGREGATE_PIPELINE_KEY = "pipeline";
    private static final String RESULT_KEY = "result";
    private static final String RUN_COMMAND_MAPREDUCE = "results";
    private static final String RUN_COMMAND_DISTINCT = "values";
    private static final String RUN_COMMAND_COUNT = "n";
    private static final String RUN_COMMAND_GROUP = "retval";
    private static final String RUN_COMMAND_CURSOR = "cursor";
    private static final String BATCH_SIZE_KEY = "batchSize";
    private static final String COLLATION_KEY = "collation";
    private static final String MAXTIME_KEY = "maxTime";
    public MongoCursor iterator;
    public BasicDBObject queryObject;
    public int rowsToProcess = 5;

    private MongoDbConnection connection;

    private Map<String, Object> parameters;

    public List<?> commandResults;

    public MongoDbQueryWrapper(String queryString, MongoDbConnection connection, Map<String, Object> parameters) throws JRException {
        this.connection = connection;
        this.parameters = parameters;
        processQuery(queryString);
    }

    public void processQuery(String queryString) throws JRException {
        logger.info("Processing mongoDB query");
        if (queryString != null && queryString.isEmpty())
            throw new JRException("Query is empty");
        if (queryString.startsWith("\""))
            queryString = queryString.substring(1, queryString.length());
        if (queryString.endsWith("\"")) {
            queryString = queryString.substring(0, queryString.length() - 1);
        }
        Object parseResult = BasicDBObject.parse(queryString);
        if (logger.isDebugEnabled()) {
            logger.debug("Query: " + queryString);
        }
        if (!(parseResult instanceof BasicDBObject))
            throw new JRException("Unsupported type: " + parseResult.getClass().getName());
        this.queryObject = (BasicDBObject) parseResult;
        fixQueryObject(this.queryObject, this.parameters);

        if (this.queryObject.containsField("runCommand")) {
            BasicDBObject command = (BasicDBObject) this.queryObject.removeField("runCommand");
            if (command.containsField("aggregate")) {
                runAggregate(command.removeField("pipeline"), command.getString("aggregate"));
                logger.warn("runCommand aggregations are deprecated ! use API Driven query for aggregation in future");
            } else {
                runCommand(this.queryObject.removeField("runCommand"));
            }
        } else {
            createIterator();
        }

        if (this.queryObject.containsField("rowsToProcess")) {
            Integer value = processInteger(this.queryObject.get("rowsToProcess"));
            if (value != null) {
                this.rowsToProcess = value.intValue();
            }
        }
        if (this.rowsToProcess == 0) {
            this.rowsToProcess = Integer.MAX_VALUE;
        }
    }

    private Object fixQueryObject(BasicDBObject queryObjectToFix, Map<String, Object> reportParameters) {
        Set<String> keySet = queryObjectToFix.keySet();
        if (keySet.size() == 1) {
            String key = keySet.iterator().next();
            if (reportParameters.containsKey(key) && queryObjectToFix.get(key) == null) {
                return reportParameters.get(key);
            }
        }
        for (String key : queryObjectToFix.keySet()) {
            Object value = queryObjectToFix.get(key);
            if (value instanceof BasicDBObject) {
                queryObjectToFix.put(key, fixQueryObject((BasicDBObject) value, reportParameters));
            }
        }
        return queryObjectToFix;
    }

    private Object getOutputCommand(Document commandResult) {
        Object resultObject = null;
        if (commandResult.get("result") != null) {
            resultObject = commandResult.get("result");
        } else if (commandResult.get("results") != null) {
            resultObject = commandResult.get("results");
        } else if (commandResult.get("values") != null) {
            resultObject = commandResult.get("values");
        } else if (commandResult.get("retval") != null) {
            resultObject = commandResult.get("retval");
        } else if (commandResult.get("n") != null) {
            resultObject = commandResult.get("n");
        } else if (commandResult.get("cursor") != null) {
            resultObject = commandResult.get("cursor");
        }
        return resultObject;
    }

    private List<?> convertObjectToList(Object resultObject) {
        List<?> listObjects = null;
        if (resultObject instanceof Integer) {
            listObjects = (List) BasicDBObject.parse("[{'total':'" + resultObject.toString() + "'}]");
        } else if (resultObject instanceof String) {
            listObjects = (List) BasicDBObject.parse("[{'total':'" + resultObject.toString() + "'}]");
        } else if (resultObject instanceof BasicDBList) {
            BasicDBList arrayObject = (BasicDBList) resultObject;
            boolean isTuple = true;
            for (int i = 0; i < arrayObject.size(); i++) {
                if (arrayObject.get(i) instanceof BasicDBObject) {
                    isTuple = false;
                    break;
                }
            }
            if (!isTuple) {
                listObjects = (List) resultObject;
            } else {
                StringBuilder stringBuilder = new StringBuilder();

                stringBuilder.append("[");
                for (int j = 0; j < arrayObject.size(); j++) {
                    if (j != 0)
                        stringBuilder.append(",");
                    stringBuilder.append("{'id':'" + j + "', 'value':'" + arrayObject.get(j) + "'}");
                }
                stringBuilder.append("]");
                listObjects = (List) BasicDBObject.parse(stringBuilder.toString());
            }
        }
        return listObjects;
    }

    private void runAggregate(Object commandValue, String collectionName) throws JRException {
        if (!(commandValue instanceof BasicDBList)) {
            throw new JRException("Command must be a valid BSON object");
        }

        ArrayList<Bson> commandObject = new ArrayList<>(((BasicDBList) commandValue).size());
        ((BasicDBList) commandValue).forEach(command -> commandObject.add((Bson) command));


        if (logger.isDebugEnabled()) {
            logger.debug("Command object: " + commandObject);
        }
        if (collectionName != null && !collectionName.isEmpty()) {
            this.iterator = this.connection.getMongoDatabase().getCollection(collectionName).aggregate(commandObject, BasicDBObject.class).iterator();
        } else {
            this.iterator = this.connection.getMongoDatabase().aggregate(commandObject, BasicDBObject.class).iterator();
        }
    }


    private void runCommand(Object commandValue) throws JRException {
        if (!(commandValue instanceof BasicDBObject)) {
            throw new JRException("Command must be a valid BSON object");
        }
        BasicDBObject commandObject = (BasicDBObject) commandValue;
        if (logger.isDebugEnabled()) {
            logger.debug("Command object: " + commandObject);
        }

        Document commandResult = this.connection.getMongoDatabase().runCommand((Bson) commandObject);

        if ( commandResult.get("ok") != null && commandResult.get("ok").toString().equalsIgnoreCase("0")) {
            throw new JRException("RunCommand ErrorMessage");
        }
        if (logger.isInfoEnabled()) {
            logger.info(commandResult.toJson());
        }
        Object resultObject = getOutputCommand(commandResult);
        if (resultObject == null) {
            throw new JRException("No results");
        }
        if (logger.isInfoEnabled()) {
            logger.info(resultObject);
        }
        this.commandResults = convertObjectToList(resultObject);
        if (logger.isDebugEnabled()) {
            logger.debug("Result List: " + resultObject);
        }
    }

    private void createIterator() throws JRException {
        if (logger.isInfoEnabled()) {
            logger.info(this.queryObject.toString());
        }
        if (!this.queryObject.containsField("collectionName")) {
            throw new JRException("\"collectionName\" must be part of the query object");
        }
        BasicDBObject findQueryObject = (BasicDBObject) this.queryObject.get("findQuery");
        if (findQueryObject == null) {
            findQueryObject = new BasicDBObject();
        }
        if (this.queryObject.containsField("findQueryRegEx")) {
            BasicDBObject regExpObject = (BasicDBObject) this.queryObject.get("findQueryRegEx");


            for (String key : regExpObject.keySet()) {
                String value = (String) regExpObject.get(key);
                if (value.startsWith("/")) {
                    value = value.substring(1, value.length());
                } else {
                    throw new JRException("Regular expressions must start with: /");
                }
                if (!value.contains("/")) {
                    throw new JRException("No ending symbol found: /");
                }
                int index = value.lastIndexOf("/");
                String flags = null;
                if (index == value.length() - 1) {
                    value = value.substring(0, index);
                } else {
                    flags = value.substring(index + 1, value.length());
                    value = value.substring(0, index);
                }
                findQueryObject.put(key, Pattern.compile(((flags != null) ? ("(?" + flags + ")") : "") + value));
            }
        }
        MongoCollection collection = this.connection.getMongoDatabase().getCollection((String) this.queryObject.removeField("collectionName"));
        Integer limitValue = Integer.valueOf(0);
        Integer batchSize = Integer.valueOf(0);
        Long maxTime = Long.valueOf(0L);

        Collation collation = null;
        if (this.queryObject.containsField("limit")) {
            limitValue = processInteger(Integer.valueOf(this.queryObject.getInt("limit", 0)));
        }
        if (this.queryObject.containsField("batchSize")) {
            batchSize = processInteger(Integer.valueOf(this.queryObject.getInt("batchSize", 0)));
        }
        if (this.queryObject.containsField("maxTime")) {
            maxTime = Long.valueOf(this.queryObject.getLong("maxTime", 0L));
        }
        if (this.queryObject.containsField("collation")) {
            if (this.queryObject.get("collation") == null) {
                throw new JRException("Collation document error: no document !");
            }
            Document col = Document.parse(this.queryObject.getString("collation"));
            if (col.getString("locale") == null) {
                throw new JRException("Collation document require locale to be present.");
            }


            collation = Collation.builder().locale(col.getString("locale")).collationStrength(CollationStrength.fromInt(col.getInteger("strength", 3))).caseLevel(Boolean.valueOf(col.getBoolean("caseLevel", false))).collationCaseFirst(CollationCaseFirst.fromString((String) col.get("caseFirst", "off"))).numericOrdering(Boolean.valueOf(col.getBoolean("numericOrdering", false))).collationAlternate(CollationAlternate.fromString((String) col.get("alternate", "non-ignorable"))).collationMaxVariable(CollationMaxVariable.fromString((String) col.get("maxVariable", "punct"))).backwards(Boolean.valueOf(col.getBoolean("backwards", false))).normalization(Boolean.valueOf(col.getBoolean("normalization", false))).build();
        }


        if (this.queryObject.containsField("mapReduce")) {

            Object value = this.queryObject.removeField("mapReduce");
            if (!(value instanceof BasicDBObject)) {
                logger.error("MapReduce value must be a valid JSON object");
            } else {
                BasicDBObject mapReduceObject = (BasicDBObject) value;
                String map = validateProperty(mapReduceObject, "map");
                String reduce = validateProperty(mapReduceObject, "reduce");


                Object queryParameter = mapReduceObject.get("query");
                if (logger.isInfoEnabled()) {
                    logger.info(queryParameter);
                }
                BasicDBObject queryValue = null;
                if (queryParameter != null) {
                    queryValue = BasicDBObject.parse(String.valueOf(queryParameter));
                }

                Object outObject = mapReduceObject.get("out");
                if (outObject == null) ;


                String collectionName = null;
                Object outDb = null;
                MapReduceAction outputType = null;
                boolean hasOutputType = false;
                if (logger.isDebugEnabled()) {
                    logger.debug("Out object: " + outObject + ". Type: " + outObject.getClass().getName());
                }
                if (outObject instanceof String) {
                    collectionName = String.valueOf(outObject);
                } else if (outObject instanceof BasicDBObject) {
                    BasicDBObject outDbObject = (BasicDBObject) outObject;
                    outDb = outDbObject.removeField("db");
                    Iterator<String> keysIterator = outDbObject.keySet().iterator();
                    String type = null;
                    if (keysIterator.hasNext()) {
                        type = keysIterator.next();
                        collectionName = String.valueOf(outDbObject.get(type));
                    } else {
                        throw new JRException("\"out\" object cannot be empty");
                    }
                    type = type.toUpperCase();
                    outputType = MapReduceAction.valueOf(type);
                    if (outputType == null) {
                        throw new JRException("Unknow output type: " + type);
                    }
                    hasOutputType = true;
                    if (logger.isInfoEnabled()) {
                        logger.info(outDbObject.toString());
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("outobject: " + outDbObject);
                        logger.debug("collectionName: " + collectionName);
                        logger.debug("outputType: " + outputType);
                    }
                } else {
                    throw new JRException("Unsupported type for \"out\": " + outObject.getClass().getName());
                }


                MapReduceIterable mapReduceIterable = collection.mapReduce(map, reduce, BasicDBObject.class).action(hasOutputType ? outputType : MapReduceAction.REPLACE).databaseName((outDb != null) ? String.valueOf(outDb) : this.connection.getMongoDatabase().getName()).collectionName(collectionName).finalizeFunction((String) mapReduceObject.removeField("finalize")).sharded(false).filter((Bson) queryValue).batchSize(batchSize.intValue()).limit(limitValue.intValue()).sort((Bson) this.queryObject.get("sort")).maxTime(maxTime.longValue(), TimeUnit.MILLISECONDS).collation(collation);
                this.iterator = mapReduceIterable.iterator();
            }
        } else if (this.queryObject.containsField("aggregate")) {
            Object commandValue = this.queryObject.get("aggregate");
            ArrayList<Bson> commandObject = new ArrayList<>(((BasicDBList) commandValue).size());
            ((BasicDBList) commandValue).forEach(command -> commandObject.add((Bson) command));


            AggregateIterable aggregateIterable = collection.aggregate(commandObject, BasicDBObject.class).batchSize(batchSize.intValue()).maxTime(maxTime.longValue(), TimeUnit.MILLISECONDS).collation(collation);
            this.iterator = aggregateIterable.iterator();


        } else {


            FindIterable findIterable = collection.find((Bson) findQueryObject, BasicDBObject.class).projection((Bson) this.queryObject.get("findFields")).limit(limitValue.intValue()).collation(collation).batchSize(batchSize.intValue()).maxTime(maxTime.longValue(), TimeUnit.MILLISECONDS).sort((Bson) this.queryObject.get("sort"));
            this.iterator = findIterable.iterator();
        }
    }


    private Integer processInteger(Object value) {
        try {
            if (value instanceof Integer) {
                return (Integer) value;
            }
            return Integer.valueOf(Integer.parseInt((String) value));
        } catch (Exception e) {
            logger.error(e);

            return null;
        }
    }

    public String validateProperty(BasicDBObject object, String name) throws JRException {
        Object value = object.get(name);
        if (value == null) {
            throw new JRException(name + " can't be null");
        }
        return String.valueOf(value);
    }
}
