
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
        MapReduce mr = new MapReduce(); //instance of MapReduce (map and reduce)


        //receiving messages (MapInput) and sending messages (MapOutput) via StreamObserver
        @Override
        public StreamObserver<MapInput> map(StreamObserver<MapOutput> mapOutputStreamObserver) {//grpc.stub library
            System.out.println("New Map");

            return new StreamObserver<MapInput>() {

                //methods of streamObserver<V>: onNext(), onError() and onCompleted()
                @Override
                public void onNext(MapInput requestMessage) { //receiving message of type MapInput
                    System.out.println("Map: " + requestMessage.getInputfilepath());

                    //trying to map the request(MapInput) and sending a MapOutput-message
                    try {
                        MapReduce.map(requestMessage.getInputfilepath());
                        System.out.println("Map: " + requestMessage.getInputfilepath() + " done");
                        int chunkNumber = Integer.parseInt(new File(requestMessage.getInputfilepath()).getName().substring(5, 8));//extracting String "001" from "chunk001.txt"
                        mapOutputStreamObserver.onNext(MapOutput.newBuilder().setJobstatus(2).build()); //2 for successful message
                    } catch (IOException e) {
                        System.err.println("Error during map operation: " + e.getMessage());
                        mapOutputStreamObserver.onNext(MapOutput.newBuilder().setJobstatus(-1).build()); //-1 if it fails
                    }
                }

                @Override
                public void onError(Throwable t) { //error handling
                    mapOutputStreamObserver.onError(t);
                    System.out.println("Error: " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    mapOutputStreamObserver.onCompleted();
                }
            };
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
