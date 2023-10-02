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
   Map<String, Integer> jobStatus = new HashMap<String, Integer>();

   public  void requestMap(String ip, Integer portnumber, String inputfilepath, String outputfilepath) throws InterruptedException {
      
       try {
         StreamObserver<MapOutput> responseObserver = new StreamObserver<MapOutput>() {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(ip,portnumber).usePlaintext().build();
            AssignJobGrpc.AssignJobStub stub = AssignJobGrpc.newStub(channel);
            StreamObserver<MapOutput> responseObserver = new StreamObserver<>();
            @Override
            public void onNext(MapOutput response) {
               if (response.getJobstatus() == 2) {
                  System.out.println("Map task completed");
               }
            }

            @Override
            public void onError(Throwable t) {
               Logger.getLogger(MrClient.class.getName()).log(Level.WARNING, "RPC failed", t);
            }

            @Override
            public void onCompleted() {
               System.out.println("Server is done!");

            }
         };

         StreamObserver<MapInput> requestStreamObserver = blockingStub.map(responseObserver);
         requestStreamObserver.onNext(request);
         requestStreamObserver.onCompleted();
      } catch (StatusRuntimeException e) {
         Logger.getLogger(MrClient.class.getName()).log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      }

   }

   public int requestReduce(String ip, Integer portnumber, String inputfilepath, String outputfilepath) {
       
      ReduceInput request = ReduceInput.newBuilder().setIp(ip).setPort(portnumber)
              .setInputfilepath(inputfilepath)
              .setOutputfilepath(outputfilepath).build();

      try {
         ReduceOutput response = blockingStub.reduce(request);
         return response.getJobstatus();
      }catch (StatusRuntimeException e){
         Logger.getLogger(MrClient.class.getName()).log(Level.WARNING, "RPC failed: {0}", e.getStatus());
         return -1;
      }
   }
   public static void main(String[] args) throws Exception {// update main function if required

      String ip = args[0];
      Integer mapport = Integer.parseInt(args[1]);
      Integer reduceport = Integer.parseInt(args[2]);
      String inputfilepath = args[3];
      String outputfilepath = args[4];
      String jobtype = null;
      MrClient client = new MrClient();
      int response = 0;

      MapReduce mr = new MapReduce();
      String chunkpath = mr.makeChunks(inputfilepath);
      Integer noofjobs = 0;
      File dir = new File(chunkpath);
      File[] directoyListing = dir.listFiles();
      if (directoyListing != null) {
         for (File f : directoyListing) {
            if (f.isFile()) {
               noofjobs += 1;
               client.jobStatus.put(f.getPath(), 1);

            }

         }
      }
      client.requestMap(ip, mapport, inputfilepath, outputfilepath);

      Set<Integer> values = new HashSet<Integer>(client.jobStatus.values());
      if (values.size() == 1 && client.jobStatus.containsValue(2)) {

         response = client.requestReduce(ip, reduceport, chunkpath + "/map", outputfilepath);
         if (response == 2) {

            System.out.println("Reduce task completed!");

         } else {
            System.out.println("Try again! " + response);
         }

      }

   }

}
