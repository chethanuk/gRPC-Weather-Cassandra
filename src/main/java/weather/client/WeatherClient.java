package weather.client;

import com.proto.blog.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class WeatherClient {

    public static void main(String[] args) {

        System.out.println("Hello, this is Blog Client");
        WeatherClient client = new WeatherClient();

        client.run();
    }

    private void run() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50054)
                .usePlaintext()
                .build();

        System.out.println("Connected to Channel");

        WeatherServiceGrpc.WeatherServiceBlockingStub weatherServiceBlockingStub = WeatherServiceGrpc.newBlockingStub(channel);

        AvgCityTempResponse avgCityTempResponse =
                weatherServiceBlockingStub.avgCityTemp(AvgCityTempRequest.newBuilder()
                        .setCity("bangalore")
                        .build());

        System.out.println("Average Temp of city: " + avgCityTempResponse.toString());

        InsertAvgCityTempResponse insertAvgCityTempResponse =
                weatherServiceBlockingStub.insertAvgCityTemp(InsertAvgCityTempRequest.newBuilder()
                        .setDate("2019/05/12")
                        .setAvgTemp(35.5)
                        .build());


    }
}
