/* 
* Client program to request for map and reduce functions from the Server
*/

package io.grpc.filesystem.task3;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.task3.proto.AssignJobGrpc;
import com.task3.proto.MapInput;
import com.task3.proto.ReduceInput;
import com.task3.proto.MapOutput;
import com.task3.proto.ReduceOutput;
import io.grpc.filesystem.task2.*;

import java.io.*;
import java.nio.charset.Charset;

public class MrClient {
   Map<String, Integer> jobStatus = new HashMap<>();

   public void requestMap(String ip, Integer portnumber, String inputfilepath, String outputfilepath) throws InterruptedException {
      ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, portnumber).usePlaintext().build(); //channel to communicate with the gRPC server
      AssignJobGrpc.AssignJobStub stub = AssignJobGrpc.newStub(channel); //client stub for asynchronous method

      //StreamObserver to map the stream (response) from server
      StreamObserver<MapInput> mapInputStreamObserver = stub.map(new StreamObserver<MapOutput>() {
         @Override
         public void onNext(MapOutput value) {
            if (value.getJobstatus() == 2) {//jobstatus = 2
               System.out.println("Map is completed! ");
               jobStatus.put(inputfilepath, 2);//update status
            }
         }

         @Override
         public void onError(Throwable t) {
            System.out.println("Error!: " + t.getMessage());
         } //error handling

         @Override
         public void onCompleted() {
            }
      });

      File inputFile = new File(inputfilepath); //new object from inputfilepath
      //sending message to server (with required data).
      MapInput mapInputMessage = MapInput.newBuilder().setInputfilepath(inputFile.getAbsolutePath())
              .setOutputfilepath(outputfilepath)
              .setIp(ip)
              .setPort(portnumber)
              .build();

      mapInputStreamObserver.onNext(mapInputMessage);
      mapInputStreamObserver.onCompleted();
      channel.awaitTermination(5, TimeUnit.SECONDS); //terminate channel connection
   }

   public int requestReduce(String ip, Integer portnumber, String inputfilepath, String outputfilepath) {
      ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, portnumber).usePlaintext().build(); //channel to communicate with server
      AssignJobGrpc.AssignJobBlockingStub stub = AssignJobGrpc.newBlockingStub(channel); //stub for communicating with server
      ReduceInput reduceInput = ReduceInput.newBuilder().setInputfilepath(inputfilepath).setOutputfilepath(outputfilepath).build(); //reduceInput message to send to server

      return stub.reduce(reduceInput).getJobstatus(); //return message and set status

   }

   public static void main(String[] args) throws Exception {// update main function if required
      String ip = args[0];
      Integer mapport = Integer.parseInt(args[1]);
      Integer reduceport = Integer.parseInt(args[2]);
      String inputfilepath = args[3];
      String outputfilepath = args[4];

      MrClient client = new MrClient();
      MapReduce mr = new MapReduce();  // Assumed to be an existing utility class

      String chunkpath = mr.makeChunks(inputfilepath); // Method to split input into chunks
      File dir = new File(chunkpath);
      File[] directoryListing = dir.listFiles();
      if (directoryListing != null) {
         for (File chunkFile : directoryListing) {
            if (chunkFile.isFile()) {
               client.jobStatus.put(chunkFile.getPath(), 1);
               client.requestMap(ip, mapport, chunkFile.getPath(), dir.getPath());
            }
         }
      }

      Set<Integer> uniqueStatuses = new HashSet<>(client.jobStatus.values());
      if (uniqueStatuses.size() == 1 && client.jobStatus.containsValue(2)) {
         int response = client.requestReduce(ip, reduceport, chunkpath, outputfilepath);
         if (response == 2) {
            System.out.println("Reduce task completed!");
         } else {
            System.out.println("Try again! " + response);
         }
      }
   }
}
