
/* 
* gRPC server node to accept calls from the clients and serve based on the method that has been requested
*/

package io.grpc.filesystem.task3;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.task3.proto.AssignJobGrpc;
import com.task3.proto.MapInput;
import com.task3.proto.ReduceInput;
import com.task3.proto.MapOutput;
import com.task3.proto.ReduceOutput;
import io.grpc.filesystem.task2.*;

public class MrMapServer {

    private Server server;

    private void start(int port) throws IOException {
        server = ServerBuilder.forPort(port).addService(new MrMapServerImpl()).build().start();
        System.out.println("Listening on: " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("Terminating the server at port: " + port);
                try {
                    server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
            }
        });
    }

    static class MrMapServerImpl extends AssignJobGrpc.AssignJobImplBase {
        MapReduce mr = new MapReduce();

         private static final Logger logger = Logger.getLogger(MrMapServer.MrMapServerImpl.class.getName());

        private final Object lock = new Object();

        private int count = 1;

        private final List<MapInput> receivedMapInputs = new ArrayList<>();
        private ConcurrentMap<MapInput, List<MapOutput>> mapResults = new ConcurrentHashMap<>();

        @Override
        public StreamObserver<MapInput> map(StreamObserver<MapOutput> responseObserver){
            return new StreamObserver<MapInput>() {
                @Override
                public void onNext(MapInput request) {
                    List<MapOutput> results = performMap(request);
                    mapResults.put(request, results);

                    MapOutput response = MapOutput.newBuilder().setJobstatus(2).build();
                    responseObserver.onNext(response);
                }


                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
        private List<MapOutput> performMap(MapInput request){
            List<MapOutput> results = new ArrayList<>();
            for (int i = 0; i <= 12; i++) {
                MapOutput result = MapOutput.newBuilder().setJobstatus(1).build();
                results.add(result);
            }
            return results;
        }


    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final MrMapServer mrServer = new MrMapServer();
        for (String i : args) {

            mrServer.start(Integer.parseInt(i));

        }
        mrServer.server.awaitTermination();
    }

}
