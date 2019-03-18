package weather.server;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.proto.blog.*;
import io.grpc.stub.StreamObserver;
import org.bson.Document;

import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

import static com.datastax.driver.core.Cluster.builder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class WeatherServerImpl extends WeatherServiceGrpc.WeatherServiceImplBase {

//    // Create a mongo Client: Used in rest of methods to access mongoDB
//    private MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
//    // Access the DB if exist or else it will create a DB
//    private MongoDatabase mongoDatabase = mongoClient.getDatabase("weather");
//    // bson Document
//    // get Collection named "blog
//    private MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("city");

    Cluster cluster;
    Session session;
    ResultSet results;
    Row rows;


    private JsonArray callAPIUrl(String url) {
        JsonArray jsonArray = null;
        int responseCode = 0;
        HttpURLConnection conn;
        URL urlOne = null;
        try {
            urlOne = new URL(url);

            conn = (HttpURLConnection) urlOne.openConnection();

            conn.setRequestMethod("GET");

            conn.connect();

            responseCode = conn.getResponseCode();

        } catch (Exception e) {
            System.out.println("Error in API Call");
        }

        if (responseCode != 200)
            throw new RuntimeException("HttpResponseCode: " + responseCode);
        else {

            // Scanner: to read all lines
            String inline = " ";
            try {
                Scanner sc = new Scanner(urlOne.openStream());
                while (sc.hasNext()) {
                    inline += sc.nextLine();
                }

                System.out.println("\nJSON data in string format");
                System.out.println(inline);
                sc.close();
            } catch (Exception e) {
                System.out.println("Error in Scanner");
            }


            JsonParser jsonParser = new JsonParser();

            jsonArray = (JsonArray) jsonParser.parse(inline);

            //                System.out.println(jsonObject.toString());
//            System.out.println("Json: " + jsonObject.toString());

        }

        return jsonArray;
    }

    // Override Avg Temp

    @Override
    public void avgCityTemp(AvgCityTempRequest request, StreamObserver<AvgCityTempResponse> responseObserver) {

        System.out.println("Received, Blog Request");

        // Use MongoDb and create Doc
        Document document = null;
        String woeid = null;

        String city = request.getCity();

        String apiOneURl = "https://www.metaweather.com/api/location/search/?query=" + city;

        System.out.println("URL: " + apiOneURl);

        JsonObject jsonObject = (JsonObject) callAPIUrl(apiOneURl).get(0);

        woeid = jsonObject.get("woeid").getAsString();

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        Date date = new Date();
        System.out.println(dateFormat.format(date));
        String formatDate = dateFormat.format(date).toString();

        String urlTwo = "https://www.metaweather.com/api/location/" + woeid + "/" + dateFormat.format(date);
        System.out.println("URL2: " + urlTwo);

        JsonArray jsonArray = callAPIUrl(urlTwo);
        System.out.println("115: jsonArray " + jsonArray);

        ArrayList<Double> the_temp = new ArrayList<Double>();

        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject jsonObject1 = (JsonObject) jsonArray.get(i);
            if (jsonObject1.has("the_temp")) {
                the_temp.add(jsonObject1.get("the_temp").getAsDouble());
            }

        }

        // Find the avg Temp
        Double avgTemp = the_temp.stream().mapToDouble(i -> i).average().orElse(0);

        responseObserver.onNext(AvgCityTempResponse.newBuilder()
                .setAvgTemp(avgTemp)
                .setDate(formatDate)
                .build());

        responseObserver.onCompleted();
    }

//    @Override
//    public void readAvgCityTemp(ReadAvgCityTempRequest request, StreamObserver<ReadAvgCityTempResponse> responseObserver) {
//
//        // connect to our Cassandra cluster and create a session instance.
//        // running a cluster instead of a single instance
//        // retry policy determines the default behavior to adopt when a request either times out or a node is unavailable
//        // on a read timeout, when enough replicas have replied but the data wasn’t received.
//        // on a write timeout, if we timeout while writing the log used by batch statements.
//        cluster = builder()
//                .addContactPoint("127.0.0.1")
//                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
//                // A load balancing policy will determine which node it is to run a query. Since a client can read or write to any node, sometimes that can be inefficient.
//                // If a node receives a read or write owned on another node, it will coordinate that request for the client.
//                .withLoadBalancingPolicy(
//                        new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
//                .build();
//        session = cluster.connect("example");
//
//        String date = request.getDate();
//
//        // Use select to get the user we just entered
//        Statement select = QueryBuilder.select().all().from("example", "avgTable")
//                .where(eq("date", date));
//        results = session.execute(select);
//        for (Row row : results) {
//            System.out.format("%s %d \n", row.getString("firstname"),
//                    row.getInt("age"));
//        }
//
//        Double avgTemp = request.getAvgTemp();
//
//        session.execute(boundStatement.bind(date, avgTemp));
//
//        responseObserver.onNext(ReadAvgCityTempResponse.newBuilder()
//                .setAvgTemp(avgTemp)
//                .setDate(date)
//                .build());
//
//        responseObserver.onCompleted();
//
//    }

    @Override
    public void insertAvgCityTemp(InsertAvgCityTempRequest request, StreamObserver<InsertAvgCityTempResponse> responseObserver) {
        cluster = builder()
                .addContactPoint("127.0.0.1")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
                .build();
        session = cluster.connect("example");


        // Insert one record into the users table
        PreparedStatement statement = session.prepare(
                "INSERT INTO avgtemp" + "(date, avgtemp)"
                        + "VALUES (?,?);");

        BoundStatement boundStatement = new BoundStatement(statement);

        String date = request.getDate();
        Double avgTemp = request.getAvgTemp();

        session.execute(boundStatement.bind(date, avgTemp));

        responseObserver.onNext(InsertAvgCityTempResponse.newBuilder()
                .setAvgTemp(avgTemp)
                .setDate(date)
                .build());

        responseObserver.onCompleted();
    }
}

