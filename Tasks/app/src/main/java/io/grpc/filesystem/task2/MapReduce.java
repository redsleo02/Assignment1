/*
 * the MapReduce functionality implemented in this program takes a single large text file to map i.e. split it into small chunks and then assign 1 to all the found words
 * then reduces by adding count values to each unique words
*/

package io.grpc.filesystem.task2;

import java.util.stream.Collectors;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Map;
import java.util.Timer;

import io.grpc.filesystem.task2.Mapper;

public class MapReduce {

    public static String makeChunks(String inputFilePath) throws IOException {
        int count = 1;
        int size = 500;
        File f = new File(inputFilePath);
        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String l = br.readLine();

            while (l != null) {
                File newFile = new File(f.getParent() + "/temp", "chunk"
                        + String.format("%03d", count++) + ".txt");
                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
                    int fileSize = 0;
                    while (l != null) {
                        byte[] bytes = (l + System.lineSeparator()).getBytes(Charset.defaultCharset());
                        if (fileSize + bytes.length > size)
                            break;
                        out.write(bytes);
                        fileSize += bytes.length;
                        l = br.readLine();
                    }
                }
            }
        }
        return f.getParent() + "/temp";

    }

    /**
     * @param inputfilepath
     * @throws IOException
     */
    public static void map(String inputfilepath) throws IOException {

        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
        String line;
        int chunkCount = 1;

        while ((line = br.readLine()) != null) {
            String[] words = line.split("\\s+");
            for (String word : words) {
                // Filter out punctuations and non-alphanumeric characters
                word = word.replaceAll("\\p{Punct}", "");
                word = word.replaceAll("^[^a-zA-Z0-9]", "");

                // Check if the word is not empty after filtering
                if (!word.isEmpty()) {
                    // Create a key-value pair with word and initial count 1
                    Mapper.emitIntermediate(word, "1");
                }
            }
        }
    }
    }

    /**
     * @param inputfilepath
     * @param outputfilepath
     * @return
     * @throws IOException
     */
    public static void reduce(String inputfilepath, String outputfilepath) throws IOException {
        // Create a HashMap to store word counts
        Map<String, Integer> wordCounts = new HashMap<>();

        // Get a list of all files in the map folder
        File mapFolder = new File(inputFolderPath + "/map");
        File[] mapFiles = mapFolder.listFiles();

        if (mapFiles != null) {
            // Iterate through each map file
            for (File mapFile : mapFiles) {
                try (BufferedReader br = new BufferedReader(new FileReader(mapFile))) {
                    String line;

                    // Read each line and split it into key-value pairs
                    while ((line = br.readLine()) != null) {
                        String[] keyValue = line.split(":");
                        if (keyValue.length == 2) {
                            String word = keyValue[0].trim();
                            int count = Integer.parseInt(keyValue[1].trim());

                            // Update word counts in the HashMap
                            wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
                        }
                    }
                }
            }

            // Write the unique words and their counts to the output file
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath))) {
                for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                    String word = entry.getKey();
                    int count = entry.getValue();
                    String outputLine = word + ":" + count;
                    bw.write(outputLine);
                    bw.newLine();
                }
            }
        }
    }

    /**
     * Takes a text file as an input and returns counts of each word in a text file
     * "output-task2.txt"
     * 
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException { // update the main function if required
        String inputFilePath = args[0];
        String outputFilePath = args[1];
        String chunkpath = makeChunks(inputFilePath);
        File dir = new File(chunkpath);
        File[] directoyListing = dir.listFiles();
        if (directoyListing != null) {
            for (File f : directoyListing) {
                if (f.isFile()) {

                    map(f.getPath());

                }

            }

            reduce(chunkpath, outputFilePath);

        }

    }
}
