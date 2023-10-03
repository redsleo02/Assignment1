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
        File inputFile = new File(inputfilepath);
        File tempMapFile = getMapFile(inputFile);

        List<Mapper<String, Integer>> mapperList = extractWordsFromInput(inputFile);

        writeMappersToFile(tempMapFile, mapperList);
    }


    //get path of inputfile
    private static File getMapFile(File inputFile) {
        // Determine the folder for map files and create it if it doesn't exist.
        String tempMapFolder = inputFile.getParentFile().getParent() + "/temp/map";
        new File(tempMapFolder).mkdirs();

        // Construct the name for the map file.
        String tempMapFilename = "map-" + inputFile.getName();
        return new File(tempMapFolder, tempMapFilename);
    }


    //extracting and mapping words from inputfile
    private static List<Mapper<String, Integer>> extractWordsFromInput(File inputFile) throws IOException {
        List<Mapper<String, Integer>> mapperList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] words = line.split("\\s+");
                for (String word : words) {
                    word = punctuation(word);
                    if (isWordValid(word)) {
                        mapperList.add(new Mapper<>(word, 1));
                    } else {
                        System.out.println(word);
                    }
                }
            }
        }
        return mapperList;
    }

    //removing punctuation and lowercase
    private static String punctuation(String word) {
        return word.replaceAll("\\p{Punct}", "").toLowerCase();
    }


    //boolean to check if a word is valid
    private static boolean isWordValid(String word) {
        return word.matches("^[a-zA-Z0-9]*$") && !word.isEmpty();
    }

    //writing the mapper list to file
    private static void writeMappersToFile(File file, List<Mapper<String, Integer>> mapperList) throws IOException {
        try (BufferedWriter wr = new BufferedWriter(new FileWriter(file))) {
            for (Mapper<String, Integer> mapper : mapperList) {
                wr.write(mapper.getWord() + ":" + mapper.getValue() + "\n");
            }
        }
    }


    /**
     * @param inputfilepath
     * @param outputfilepath
     * @throws IOException
     */


    //Map of words and their counts
    public static void reduce(String inputfilepath, String outputfilepath) throws IOException {
        Map<String, Integer> wordCounts = collectWordCounts(new File(inputfilepath + "/map"));
        List<Map.Entry<String, Integer>> sortedEntries = sortWordCounts(wordCounts); //sort in order
        writeSortedWordCountsToFile(sortedEntries, outputfilepath); //writing counted and sorted words to file
    }

    //collecting all the words within a Map.
    private static Map<String, Integer> collectWordCounts(File mapFolder) throws IOException {
        Map<String, Integer> wordCounts = new HashMap<>();
        for (File file : mapFolder.listFiles()) {
            collectingWordCountsFromFile(file, wordCounts);
        }
        return wordCounts;
    }

    //map with collected words from file
    private static void collectingWordCountsFromFile(File file, Map<String, Integer> wordCounts) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) { //iteration
                String[] keyValue = line.split(":"); //"key:value" format
                String word = keyValue[0].trim();
                int count = Integer.parseInt(keyValue[1].trim());
                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count); //update and counter incrementing
            }
        }
    }

    //sorting the words
    private static List<Map.Entry<String, Integer>> sortWordCounts(Map<String, Integer> wordCounts) {
        List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(wordCounts.entrySet());
        sortedEntries.sort(Map.Entry.<String, Integer>comparingByValue().reversed()); //sort entries by value
        return sortedEntries;
    }

    //the sorted words writing on outputfilepath
    private static void writeSortedWordCountsToFile(List<Map.Entry<String, Integer>> sortedEntries, String outputfilepath) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputfilepath))) {
            for (Map.Entry<String, Integer> entry : sortedEntries) {
                writer.write(entry.getKey() + ":" + entry.getValue() + "\n"); //write key and value
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
