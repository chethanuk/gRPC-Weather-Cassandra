package weather.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.io.IOException;

public class WeatherServer {

    public static void main(String[] args) throws InterruptedException, IOException {
        // Create Server
        Server server = ServerBuilder.forPort(50054)
                .addService(new WeatherServerImpl())
                .addService(ProtoReflectionService.newInstance()) // added Reflection
                .build();

        // start
        server.start();
        System.out.println("Started");

        // Shutdown: Using Runtime shutdown server [Imp: before await Termination]
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received Shutdown Request");
            server.shutdown();
            System.out.println("Successfully, Stopped Shutdown the server");
        }));

        server.awaitTermination();
    }
}
