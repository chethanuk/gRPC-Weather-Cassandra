package weather.server;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.proto.blog.AvgCityTempRequest;
import com.proto.blog.AvgCityTempResponse;
import com.proto.blog.WeatherServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.bson.Document;

import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

public class WeatherServerImpl extends WeatherServiceGrpc.WeatherServiceImplBase {

//    // Create a mongo Client: Used in rest of methods to access mongoDB
//    private MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
//    // Access the DB if exist or else it will create a DB
//    private MongoDatabase mongoDatabase = mongoClient.getDatabase("weather");
//    // bson Document
//    // get Collection named "blog
//    private MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("city");

    final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
            .build();
    final Session session = cluster.connect("example");

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

}

