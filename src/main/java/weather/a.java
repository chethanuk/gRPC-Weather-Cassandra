package weather;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import static com.datastax.driver.core.Cluster.builder;

/**
 * Testing Cassandra CRUD with datastax driver
 */
public class a {

  /*
    CREATE KEYSPACE IF NOT EXISTS example
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
CREATE TABLE IF NOT EXISTS example.avgTemp (
        date text,
        avgTemp double,
        PRIMARY KEY(date)
        );
*/

    public static void main(String[] args) {
        // Connect to the cluster and keyspace "devjavasource"
        Cluster cluster;
        Session session;
        ResultSet results;
        Row rows;

        cluster = builder()
                .addContactPoint("127.0.0.1")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                // A load balancing policy will determine which node it is to run a query. Since a client can read or write to any node, sometimes that can be inefficient.
                // If a node receives a read or write owned on another node, it will coordinate that request for the client.
                .withLoadBalancingPolicy(
                        new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
                .build();
        session = cluster.connect("example");



//        final Cluster cluster = builder().addContactPoint("127.0.0.1")
//                .build();
//        final Session session = cluster.connect("example");
//
//        System.out.println("*********Cluster Information *************");
//        System.out.println(" Cluster Name is: " + cluster.getClusterName());
//        System.out.println(" Driver Version is: " + cluster.getDriverVersion());
//        System.out.println(" Cluster Configuration is: " + cluster.getConfiguration());
//        System.out.println(" Cluster Metadata is: " + cluster.getMetadata());
//        System.out.println(" Cluster Metrics is: " + cluster.getMetrics());
//
//        // Retrieve all User details from Users table
//        System.out.println("\n*********Retrive User Data Example *************");
//
//        getUsersAllDetails(session);
//
//        // Insert new User into users table
//        System.out.println("\n*********Insert User Data Example *************");
//        session.execute("INSERT INTO example.avgTemp  ( date, avgTemp) VALUES ('2019/03/18', 45.7)");
//        session.execute("INSERT INTO example.avgTemp  ( date, avgTemp) VALUES ('2019/03/19', 43.7)");
//        getUsersAllDetails(session);
//
//        // Update user data in users table
//        System.out.println("\n*********Update User Data Example *************");
//        session.execute("update example.avgTemp set avgTemp = 46.7 where date='2019/03/18'");
//        getUsersAllDetails(session);
//
//        // Delete user from users table
//        System.out.println("\n*********Delete User Data Example *************");
//        session.execute("delete FROM example.avgTemp where date = '2019/03/18' ");
//        getUsersAllDetails(session);
//
//        // Close Cluster and Session objects
//        System.out.println("\nIs Cluster Closed :" + cluster.isClosed());
//        System.out.println("Is Session Closed :" + session.isClosed());
//        cluster.close();
//        session.close();
//        System.out.println("Is Cluster Closed :" + cluster.isClosed());
//        System.out.println("Is Session Closed :" + session.isClosed());
    }

    private static void getUsersAllDetails(final Session inSession) {
        // Use select to get the users table data
        ResultSet results = inSession.execute("SELECT * FROM example.avgTemp ");
        for (Row row : results) {
            System.out.format("%s %.2f\n", row.getString("date"), row.getDouble("avgTemp"));
        }
    }
}
