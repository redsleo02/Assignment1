
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

public class MrReduceServer {

    //handle the reducepart!
    private Server server;

    private void start(int port) throws IOException {
        server = ServerBuilder.forPort(port).addService(new MrReduceServerImpl()).build().start();
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

    //implementing
    static class MrReduceServerImpl extends AssignJobGrpc.AssignJobImplBase {
        //instance for MapReduce
        MapReduce mr = new MapReduce();

        @Override
        public void reduce(ReduceInput request, StreamObserver<ReduceOutput> responseObserver) {
            try {
                System.out.println(request.getInputfilepath());
                System.out.println(request.getOutputfilepath());
                mr.reduce(request.getInputfilepath(), request.getOutputfilepath());//calling reduce method with input and output filepath

                System.out.println("Reduce: " + request.getInputfilepath() + " done");

                //Building ReduceOutput with a responseMessage. (2 for success and 1 for fail)
                ReduceOutput responseMessage = ReduceOutput.newBuilder().setJobstatus(2).build();
                responseObserver.onNext(responseMessage); //send back to client
            } catch (Exception e) {
                ReduceOutput responseMessage = ReduceOutput.newBuilder().setJobstatus(1).build();
                responseObserver.onNext(responseMessage);
            }
            responseObserver.onCompleted(); //client to server (sending is completed)
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final MrReduceServer mrServer = new MrReduceServer();
        for (String i : args) {
            mrServer.start(Integer.parseInt(i));
        }
        mrServer.server.awaitTermination();
    }
}
