package webdata;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class IndexWriter {
    private long co = 0;
    private LinkedHashSet<String> tokensSet;
    private LinkedHashMap<String, Integer> tokensIdDict;
    private LinkedHashMap<Integer, List<Integer>> tokensTempDict; // the temp dictionary of the tokens
    private LinkedHashMap<String, Integer> productsIdDict;
    private LinkedHashMap<Integer, List<Integer>> productsTempDict; // the temp dictionary of the products
    private LinkedHashMap<Integer, String> oppositeTokensIdDict;
    private LinkedHashMap<Integer, String> oppositeProductsIdDict;
    private int collection;
    private int reviews;
    private final int MEMORY_LIMIT = 65000000;
    private final int M = 5000;
    private final int M_PRODUCTS = 5000;
    private int B = 0;
    private int B_PRODUCTS = 0;
    byte[] bytesToBytes = new byte[4];
    int[] find_minimum = new int[2];
    List<String> line_words = new ArrayList<>();

    private BufferedOutputStream frequenciesW; // frequencies file
    private BufferedOutputStream postingW; // posting lists file
    private FileWriter longStringW; // long string file
    private DataOutputStream positionsW; // hold the position of posting lists and frequencies
    private BufferedOutputStream sizesW;
    private BufferedOutputStream infoBlocksW;
    private BufferedOutputStream productPostingW; // posting lists file

    private FileWriter productLongStringW;
    private BufferedOutputStream productPositionsW; // hold the position of posting lists and frequencies
    private BufferedOutputStream productSizesW;
    private BufferedOutputStream productInfoBlocksW;
    private BufferedOutputStream locationsLongStringW; // long string file
    private Gamma gamma; // an encoder object


    /**
     * Given product review data, creates an on disk index
     * inputFile is the path to the file containing the review data
     * dir is the directory in which all index files will be created
     * if the directory does not exist, it should be created
     */
    public void write(String inputFile, String dir) {
        gamma = new Gamma();
        reviews = 0;
        collection = 0;
        tokensTempDict = new LinkedHashMap<>();
        tokensSet = new LinkedHashSet<>();
        LinkedHashSet<String> productsSet = new LinkedHashSet<>();
        productsTempDict = new LinkedHashMap<>();
        oppositeTokensIdDict = new LinkedHashMap<>();
        oppositeProductsIdDict = new LinkedHashMap<>();
        long beforeStepOne = 0;

        int counterProducts = 0;
        int[] numerator_denominator_int = new int[2];
        try {
            boolean created;
            int reviewId = 0;
            File pathAsFile = new File(dir);
            if (!Files.exists(Paths.get(dir))) {
                created = pathAsFile.mkdir();
            }
            String SORTED_FILES_DIR = Paths.get(dir, "sortedFiles").toString();
            pathAsFile = new File(SORTED_FILES_DIR);
            created = pathAsFile.mkdir();
            String SORTED_PRODUCTS_FILES_DIR = Paths.get(dir, "sortedProductsFiles").toString();
            pathAsFile = new File(SORTED_PRODUCTS_FILES_DIR);
            created = pathAsFile.mkdir();
            // creates the index files
            DataOutputStream scoresW = new DataOutputStream(new BufferedOutputStream(new FileOutputStream
                    (Paths.get(dir, "scores.bin").toString())));
            BufferedOutputStream helpfulnessW = new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "helpfulness.bin").toString()));
            BufferedOutputStream reviewLengthsW = new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "reviewLengths.bin").toString()));
            BufferedWriter tempFile = new BufferedWriter(new FileWriter(Paths.get(dir, "sortedFiles",
                    "tempFile.txt").toString()));
            // Read the input file
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            String line = reader.readLine();
            while ((line != null)) {
                if (line.startsWith("product/productId:")) {
                    reviews++;
                    reviewId++;
                    String goodProduct = line.substring(19);
                    productsSet.add(goodProduct);
                    tempFile.write(line + '\n');
                    line = reader.readLine();
                    continue;
                }
                if (line.startsWith("review/helpfulness:")) {
                    String numeric = line.substring(20);
                    String[] numerator_denominator = numeric.split("/");
                    numerator_denominator_int[0] = Integer.parseInt(numerator_denominator[0]);
                    numerator_denominator_int[1] = Integer.parseInt(numerator_denominator[1]);
                    if (numerator_denominator_int[0] > numerator_denominator_int[1]) {
                        int temp = numerator_denominator_int[0];
                        numerator_denominator_int[0] = numerator_denominator_int[1];
                        numerator_denominator_int[1] = temp;
                    }
                    byte[] numerator = intToByteArray(numerator_denominator_int[0]);
                    helpfulnessW.write(numerator);
                    byte[] denominator = intToByteArray(numerator_denominator_int[1]);
                    helpfulnessW.write(denominator);
                    line = reader.readLine();
                    continue;
                }
                if (line.startsWith("review/score:")) {
                    String score = line.substring(14, 15);
                    scoresW.writeByte(score.charAt(0) - '0');
                    line = reader.readLine();
                    continue;
                }
                if (line.startsWith("review/text:")) {
                    line = line.substring(13).toLowerCase();
                    tempFile.write(line + '\n');
                    createTermIdDictionary(line);



                }
                line = reader.readLine();
            }
            tempFile.close();
            reader.close();

            BufferedReader tempFileReader = new BufferedReader(new FileReader(Paths.get(dir, "sortedFiles",
                    "tempFile.txt").toString()));
            List<String> tmpList = new ArrayList<>(tokensSet);
            Collections.sort(tmpList);
            tokensIdDict = new LinkedHashMap<>();
            for (int i = 0; i < tmpList.size(); i++) {
                tokensIdDict.put(tmpList.get(i), i);
            }
            tokensSet.clear();
            tmpList = new ArrayList<>(productsSet);
            Collections.sort(tmpList);
            productsIdDict = new LinkedHashMap<>();
            for (int i = 0; i < tmpList.size(); i++) {
                productsIdDict.put(tmpList.get(i), i);
            }

            productsSet.clear();
            reviewId = 0;
            int tokensCounter = 0, productsCounter = 0;
            line = tempFileReader.readLine();
            while ((line != null)) {
                boolean moveLine = true;
                if (line.startsWith("product/productId:")) {
                    reviewId++;
                    line = line.substring(19);
                    if ((productsCounter + 2) >= MEMORY_LIMIT) {
                        // Write to file block tokens and posting list
                        counterProducts = writeTokensToFileProduct(B_PRODUCTS, SORTED_PRODUCTS_FILES_DIR,
                                productsTempDict, counterProducts);
                        B_PRODUCTS++;
                        productsTempDict.clear();
                        productsCounter = 0;
                    }
                    writeProducts(line, reviewId);
                    productsCounter += 2;
                    line = tempFileReader.readLine();
                    continue;
                } else {
                    countTokens(line);
                    int size = line_words.size();
                    if ((tokensCounter + size) >= MEMORY_LIMIT) {
                        // Write to file block tokens and posting list
                        if (!tokensTempDict.isEmpty()) {
                            beforeStepOne = writeTokensToFile(B, SORTED_FILES_DIR, tokensTempDict, beforeStepOne);
                            B++;
                            tokensTempDict.clear();
                            tokensCounter = 0;
                            line_words.clear();
                            moveLine = false;
                        } else {
                            reviewLengthsW.write(toBytes(size, bytesToBytes));
                            tokensCounter = writeTokens(line_words, reviewId, tokensCounter);
                        }
                    } else {
                        reviewLengthsW.write(toBytes(size, bytesToBytes));
                        tokensCounter = writeTokens(line_words, reviewId, tokensCounter);
                    }

                }
                if (moveLine) {
                    line = tempFileReader.readLine();
                }
            }
            if ((tokensCounter > 0) && !tokensTempDict.isEmpty()) {
                // Write to file block tokens and posting list
                beforeStepOne = writeTokensToFile(B, SORTED_FILES_DIR, tokensTempDict, beforeStepOne);
                B++;
                tokensTempDict.clear();
            }
            if ((productsCounter > 0) && !productsTempDict.isEmpty()) {
                // Write to file block tokens and posting list
                counterProducts = writeTokensToFileProduct(B_PRODUCTS, SORTED_PRODUCTS_FILES_DIR, productsTempDict,
                        counterProducts);
                B_PRODUCTS++;
                productsTempDict.clear();
            }
            tempFileReader.close();
            scoresW.close();
            helpfulnessW.close();
            reviewLengthsW.close();
            for (Map.Entry<String, Integer> entry : tokensIdDict.entrySet()) {
                oppositeTokensIdDict.put(entry.getValue(), entry.getKey());
            }
            tokensIdDict.clear();
            for (Map.Entry<String, Integer> entry : productsIdDict.entrySet()) {
                oppositeProductsIdDict.put(entry.getValue(), entry.getKey());
            }
            productsIdDict.clear();
            int counterFiles = sortStepOne(SORTED_FILES_DIR, beforeStepOne);
            sortStepTwo(SORTED_FILES_DIR, counterFiles, co, true);
            //create the inverted index files
            openArraysFiles(dir);
            constructorDic(dir);
            buildDicFile(SORTED_FILES_DIR);
            frequenciesW.close();
            postingW.close();
            positionsW.close();
            sizesW.close();
            infoBlocksW.close();
            counterFiles = sortStepOneProduct(SORTED_PRODUCTS_FILES_DIR);
            sortStepTwoProducts(SORTED_PRODUCTS_FILES_DIR, counterFiles, counterProducts, false);
            openArraysProductFiles(dir);
            buildDicProductFile(SORTED_PRODUCTS_FILES_DIR);
            productPostingW.close();
            productPositionsW.close();
            productSizesW.close();
            removeIndex(SORTED_FILES_DIR);
            removeIndex(SORTED_PRODUCTS_FILES_DIR);
        } catch (Exception e) {
            System.out.println("Error in write!!!");
        }
    }

    /***
     * This function converts an integer into bytes array
     * @param i - The int to convert
     * @return a bytes array of the integer
     */
    private byte[] toBytes(int i, byte[] bytes) {
        bytes[0] = (byte) (i >> 24);
        bytes[1] = (byte) (i >> 16);
        bytes[2] = (byte) (i >> 8);
        bytes[3] = (byte) (i);
        return bytes;
    }

    /***
     * This function performs the first phase of the external sorted bacsed algorithm
     * @return the number of files created
     */
    private int sortStepOne(String dir, long N){
        int filesCounter = 0;
        try {
            int left = 0;
            while (left < B) {
                int right = Math.min(left + M, B) - 1;
                int size = (right - left) + 1;
                BufferedInputStream[] buffers = new BufferedInputStream[size];
                int index = 0;
                for (int i = left; i < right + 1; i++) {
                    String path = Paths.get(dir, String.format("file%1d.bin", i)).toString();
                    BufferedInputStream file = new BufferedInputStream(new FileInputStream(path));
                    buffers[index] = file;
                    index++;
                }
                sortArrays(buffers, dir, filesCounter, N);
                left = left + M;
                filesCounter++;
            }
        } catch (Exception e) {
            System.out.println("Error in step one sort");
        }
        return filesCounter;
    }

    /***
     * This function sorts a few sorted files into one merged file
     */
    private void sortArrays(BufferedInputStream[] buffers, String dir, int filesCounter, long N) {
        N = N / 3;
        try {
            String path = Paths.get(dir, String.format("files%1d.bin", filesCounter)).toString();
            BufferedOutputStream file = new BufferedOutputStream(new FileOutputStream(path));
            int counter = 0, freq = 0, prevTermId, prevReviewId;
            int[] curReviewIds = new int[buffers.length];
            int[] curIds = new int[buffers.length];
            for (int i = 0; i < buffers.length; i++) {
                buffers[i].read(bytesToBytes, 0, 4);
                curIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                buffers[i].read(bytesToBytes, 0, 4);
                curReviewIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            }
            N = N - buffers.length;
            int reviewId = 0;
            int[] minimum = findMinimum(curIds, curReviewIds);
            int minIndex = minimum[1];
            prevTermId = minimum[0];
            prevReviewId = curReviewIds[minIndex];
            buffers[minimum[1]].read(bytesToBytes, 0, 4);
            freq += java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            while (N > 0) {
                for (int i = 0; i < buffers.length; i++) {
                    if (buffers[i].available() > 0) {
                        if ((minIndex != i) && (buffers.length > 1))
                        {
                            continue;
                        }
                        buffers[i].read(bytesToBytes, 0, 4);
                        curIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                        N--;
                        buffers[i].read(bytesToBytes, 0, 4);
                        curReviewIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                    } else {
                        curIds[i] = Integer.MAX_VALUE;
                        counter++;
                    }
                }
                if (counter == buffers.length) {
                    break;
                }
                counter = 0;
                minimum = findMinimum(curIds, curReviewIds);
                minIndex = minimum[1];
                reviewId = curReviewIds[minIndex];
                buffers[minIndex].read(bytesToBytes, 0, 4);
                int curFreq = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                if ((minimum[0] == prevTermId) && (prevReviewId == reviewId)) {
                    freq += curFreq;
                } else {
                    file.write(toBytes(prevTermId, bytesToBytes));
                    file.write(toBytes(prevReviewId, bytesToBytes));
                    file.write(toBytes(freq, bytesToBytes));
                    co+=3;
                    freq = curFreq;
                    prevTermId = minimum[0];
                    prevReviewId = reviewId;
                }
            }
            if ((minimum[0] == prevTermId) && (prevReviewId == reviewId)) {
                file.write(toBytes(prevTermId, bytesToBytes));
                file.write(toBytes(prevReviewId, bytesToBytes));
                file.write(toBytes(freq, bytesToBytes));
                co+=3;
            }
            file.close();

        } catch (Exception e) {
            System.out.println("Error in sort arrays");
        }

    }

    /***
     * This function performs the first phase of the external sorted bacsed algorithm
     * @return the number of files created
     */
    private int sortStepOneProduct(String dir) {
        int filesCounter = 0;
        try {
            int left = 0;
            while (left < B_PRODUCTS) {
                int right = Math.min(left + M_PRODUCTS, B_PRODUCTS) - 1;
                int size = (right - left) + 1;
                BufferedInputStream[] buffers = new BufferedInputStream[size];
                int index = 0;
                for (int i = left; i < right + 1; i++) {
                    String path = Paths.get(dir, String.format("file%1d.bin", i)).toString();
                    BufferedInputStream file = new BufferedInputStream(new FileInputStream(path));
                    buffers[index] = file;
                    index++;
                }
                sortArraysProducts(buffers, dir, filesCounter);
                left = left + M_PRODUCTS;
                filesCounter++;
            }
        } catch (Exception e) {
            System.out.println("Error in step one sort");
        }
        return filesCounter;
    }

    /***
     * This function sorts a few sorted files into one merged file
     */
    private void sortArraysProducts(BufferedInputStream[] buffers, String dir, int filesCounter) {
        try {
            String path = Paths.get(dir, String.format("files%1d.bin", filesCounter)).toString();
            BufferedOutputStream file = new BufferedOutputStream(new FileOutputStream(path));
            int counter = 0;
            int[] curIds = new int[buffers.length];
            for (int i = 0; i < buffers.length; i++) {
                buffers[i].read(bytesToBytes, 0, 4);
                curIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            }
            int[] minimum = findMinimumProduct(curIds);
            int minIndex = minimum[1];
            file.write(toBytes(minimum[0], bytesToBytes));
            buffers[minIndex].read(bytesToBytes, 0, 4);
            int reviewId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            file.write(toBytes(reviewId, bytesToBytes));
            while (true) {
                for (int i = 0; i < buffers.length; i++) {
                    if (buffers[i].available() > 0) {
                        if ((minIndex != i) && (buffers.length > 1))
                        {
                            continue;
                        }
                        buffers[i].read(bytesToBytes, 0, 4);
                        curIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                    } else {
                        curIds[i] = Integer.MAX_VALUE;
                        counter++;
                    }
                }
                if (counter == buffers.length) {
                    break;
                }
                counter = 0;
                minimum = findMinimumProduct(curIds);
                minIndex = minimum[1];
                file.write(toBytes(minimum[0], bytesToBytes));
                buffers[minIndex].read(bytesToBytes, 0, 4);
                reviewId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                file.write(toBytes(reviewId, bytesToBytes));
            }
            file.close();
        } catch (Exception e) {
            System.out.println("Error in sort arrays");
        }
    }

    /***
     * This function finds the minimum of the current Ids and if there is an eqality, compares also the reviewIds
     * @return The minimal termId and it's index
     */
    private int[] findMinimum(int[] curIds, int[] curReviewsIds) {
        int minToken = Integer.MAX_VALUE;
        int minReview = Integer.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i < curIds.length; i++) {
            int token = curIds[i];
            if (minToken > token) {
                minToken = token;
                minReview = curReviewsIds[i];
                minIndex = i;
            } else if (minToken == token) {
                if (minReview > curReviewsIds[i]) {
                    minReview = curReviewsIds[i];
                    minIndex = i;
                }
            }
        }
        find_minimum[0] = curIds[minIndex];
        find_minimum[1] = minIndex;
        return find_minimum;
    }

    /***
     * This function finds the minimum of the current Ids
     * @return The minimal termId and it's index
     */
    private int[] findMinimumProduct(int[] curIds) {
        int minToken = Integer.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i < curIds.length; i++) {
            int token = curIds[i];
            if (minToken > token) {
                minToken = token;
                minIndex = i;
            }
        }
        find_minimum[0] = curIds[minIndex];
        find_minimum[1] = minIndex;
        return find_minimum;
    }

    /***
     * This function preforms the second phase of the sorted based algorithm
     */
    private void sortStepTwo(String dir, int counterFiles, long N, boolean isTokens) {
        try {
            int numOfSequences = (int) Math.ceil((double) counterFiles / M);
            BufferedInputStream[] buffers = new BufferedInputStream[numOfSequences];
            int[] buffers_index = new int[numOfSequences];
            openBuffers(buffers, buffers_index, numOfSequences, dir, isTokens);
            insertToOutputBlock(buffers, buffers_index, dir, counterFiles, N, isTokens);
            // close the last buffers
            for (BufferedInputStream file : buffers) {
                file.close();
            }
        } catch (Exception e) {
            System.out.println("Error in step two");
        }
    }

    /***
     * This function opens numOfSequences buffers for reading from in the phase two sorting
     */
    private void openBuffers(BufferedInputStream[] buffers, int[] buffers_index, int numOfSequences,
                             String dir, boolean isTokens) {
        int m = isTokens ? M : M_PRODUCTS;
        int index = 0;
        for (int i = 0; i < (numOfSequences * m); i += m) {
            String path = Paths.get(dir, String.format("files%1d.bin", i)).toString();
            try {
                BufferedInputStream file = new BufferedInputStream(new FileInputStream(path));
                buffers[index] = file;
                buffers_index[index] = i;
                index++;
            } catch (Exception e) {
                System.out.println("Error in open buffer reader files");
            }
        }
    }

    /***
     * This function is part of the second phase algorithm that is responsible of writing into the sorted file
     * the termIds and ReviewIds
     */
    private void insertToOutputBlock(BufferedInputStream[] buffers, int[] buffers_index,
                                     String dir, int counterFiles, long N, boolean isTokens) {
        try {
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(
                    Paths.get(dir, "sortedFile.bin").toString()));
            // current output block to be flushed
            int k = 3;
            int[] curIds = new int[buffers.length];
            int[] curReviewsIds = new int[buffers.length];
            int freq = 0, prevTermId, prevReviewId;
            for (int i = 0; i < buffers.length; i++) {
                buffers[i].read(bytesToBytes, 0, 4);
                curIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                buffers[i].read(bytesToBytes, 0, 4);
                curReviewsIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            }
            int reviewId = 0;
            int[] minimum = findMinimum(curIds, curReviewsIds);
            int minIndex = minimum[1];
            prevTermId = minimum[0];
            prevReviewId = curReviewsIds[minIndex];
            buffers[minimum[1]].read(bytesToBytes, 0, 4);
            freq += java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            while (k < N) {
                for (int i = 0; i < buffers.length; i++) {
                    if (buffers[i].available() > 0) {
                        if ((minIndex != i) && (buffers.length > 1))
                        {
                            continue;
                        }
                        buffers[i].read(bytesToBytes, 0, 4);
                        curIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                        buffers[i].read(bytesToBytes, 0, 4);
                        curReviewsIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                    } else {
                        int cur = getNextChunk(buffers, buffers_index, minIndex, dir, counterFiles, isTokens);
                        if (cur != Integer.MAX_VALUE){
                            buffers[i].read(bytesToBytes, 0, 4);
                            curReviewsIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                        }
                        curIds[i] = cur;
                    }
                }
                minimum = findMinimum(curIds, curReviewsIds);
                minIndex = minimum[1];
                reviewId = curReviewsIds[minIndex];
                buffers[minIndex].read(bytesToBytes, 0, 4);
                int curFreq = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                if ((minimum[0] == prevTermId) && (prevReviewId == reviewId)) {
                    freq += curFreq;
                } else {
                    output.write(toBytes(prevTermId, bytesToBytes));
                    output.write(toBytes(prevReviewId, bytesToBytes));
                    output.write(toBytes(freq, bytesToBytes));
                    freq = curFreq;
                    prevTermId = minimum[0];
                    prevReviewId = reviewId;
                }
                k += 3;
            }
            if ((minimum[0] == prevTermId) && (prevReviewId == reviewId)) {
                output.write(toBytes(prevTermId, bytesToBytes));
                output.write(toBytes(prevReviewId, bytesToBytes));
                output.write(toBytes(freq, bytesToBytes));
            }
            output.write(toBytes(-1, bytesToBytes));
            output.close();
        } catch (Exception e) {
            System.out.println("Error in writing to output");
        }
    }

    /***
     * This function moves to the next file in the buffers
     */
    private int getNextChunk(BufferedInputStream[] buffers, int[] buffers_index, int minIndex,
                             String dir, int counterFiles, boolean isTokens) {
        try {
            int bufferIndex = buffers_index[minIndex];
            int m = isTokens ? M : M_PRODUCTS;
            if ((((bufferIndex + 1) % m == 0) || (bufferIndex + 1 == counterFiles))) {
                return Integer.MAX_VALUE;
            }
            bufferIndex++;
            String path = Paths.get(dir, String.format("files%1d.bin", bufferIndex)).toString();
            BufferedInputStream file = new BufferedInputStream(new FileInputStream(path));
            buffers[minIndex] = file;
            buffers_index[minIndex] = bufferIndex;
            buffers[minIndex].read(bytesToBytes, 0, 4);
            return java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
        } catch (Exception e) {
            System.out.println("Error in get next chuck");
        }
        return 0;
    }

    /***
     * This function preforms the second phase of the sorted based algorithm
     */
    private void sortStepTwoProducts(String dir, int counterFiles, int counterProducts, boolean isTokens) {
        try {
            int numOfSequences = (int) Math.ceil((double) counterFiles / M_PRODUCTS);
            BufferedInputStream[] buffers = new BufferedInputStream[numOfSequences];
            int[] buffers_index = new int[numOfSequences];
            openBuffers(buffers, buffers_index, numOfSequences, dir, isTokens);
            insertToOutputBlockProducts(buffers, buffers_index, dir, counterFiles, counterProducts, isTokens);
            // close the last buffers
            for (BufferedInputStream file : buffers) {
                file.close();
            }
        } catch (Exception e) {
            System.out.println("Error in step two");
        }
    }

    /***
     * This function is part of the second phase algorithm that is responsible of writing into the sorted file
     * the termIds and ReviewIds
     */
    private void insertToOutputBlockProducts(BufferedInputStream[] buffers, int[] buffers_index,
                                             String dir, int counterFiles, int N, boolean isTokens) {
        try {
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(
                    Paths.get(dir, "sortedFile.bin").toString()));
            // current output block to be flushed
            int k = 2, minIndex;
            int[] curIds = new int[buffers.length];
            for (int i = 0; i < buffers.length; i++) {
                buffers[i].read(bytesToBytes, 0, 4);
                curIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            }
            int[] minimum = findMinimumProduct(curIds);
            minIndex = minimum[1];
            output.write(toBytes(minimum[0], bytesToBytes));
            buffers[minIndex].read(bytesToBytes, 0, 4);
            int reviewId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            output.write(toBytes(reviewId, bytesToBytes));
            while (k < N) {
                for (int i = 0; i < buffers.length; i++) {
                    if (buffers[i].available() > 0) {
                        if ((minIndex != i) && (buffers.length > 1))
                        {
                            continue;
                        }
                        buffers[i].read(bytesToBytes, 0, 4);
                        curIds[i] = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                    } else {
                        int cur = getNextChunk(buffers, buffers_index, minIndex, dir, counterFiles, isTokens);
                        curIds[i] = cur;
                    }
                }
                minimum = findMinimumProduct(curIds);
                minIndex = minimum[1];
                output.write(toBytes(minimum[0], bytesToBytes));
                buffers[minIndex].read(bytesToBytes, 0, 4);
                reviewId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                output.write(toBytes(reviewId, bytesToBytes));
                k += 2;
            }
            output.write(toBytes(-1, bytesToBytes));
            output.close();
        } catch (Exception e) {
            System.out.println("Error in writing to output products");
        }
    }

    /***
     * This function writes the current hashmap of tokens in the main memory into a temp file
     */
    private long writeTokensToFile(int counterFiles, String dir, LinkedHashMap<Integer, List<Integer>> dict, long counter) {
        try {
            List<Integer> ids = new ArrayList<>(dict.keySet());
            Collections.sort(ids);
            String path = Paths.get(dir, String.format("file%1d.bin", counterFiles)).toString();
            BufferedOutputStream file = new BufferedOutputStream(new FileOutputStream(path));
            for (int termId : ids) {
                int freq = 1;
                int prevReviewId = dict.get(termId).get(0);
                for (int i = 1; i < dict.get(termId).size(); i++) {
                    int curReviewId = dict.get(termId).get(i);
                    if (prevReviewId == curReviewId) {
                        freq++;
                    } else {
                        file.write(toBytes(termId, bytesToBytes));
                        file.write(toBytes(prevReviewId, bytesToBytes));
                        file.write(toBytes(freq, bytesToBytes));
                        counter += 3;
                        freq = 1;
                    }
                    prevReviewId = curReviewId;
                }
                file.write(toBytes(termId, bytesToBytes));
                file.write(toBytes(prevReviewId, bytesToBytes));
                file.write(toBytes(freq, bytesToBytes));
                counter += 3;
            }
            file.close();
        } catch (Exception e) {
            System.out.println("Error in sorted file");
        }
        return counter;
    }

    /***
     * This function writes the current hashmap of products in the main memory into a temp file
     */
    private int writeTokensToFileProduct(int counterFiles, String dir, LinkedHashMap<Integer, List<Integer>> dict,
                                         int counterProducts) {
        try {
            List<Integer> ids = new ArrayList<>(dict.keySet());
            Collections.sort(ids);
            String path = Paths.get(dir, String.format("file%1d.bin", counterFiles)).toString();
            BufferedOutputStream file = new BufferedOutputStream(new FileOutputStream(path));
            for (int termId : ids) {
                for (int reviewIdOfTermId : dict.get(termId)) {
                    file.write(toBytes(termId, bytesToBytes));
                    file.write(toBytes(reviewIdOfTermId, bytesToBytes));
                    counterProducts += 2;
                }
            }
            file.close();
        } catch (Exception e) {
            System.out.println("Error in sorted file");
        }
        return counterProducts;
    }


    /***
     * Opens all the product dictionary files
     * @param dir - The directory in which to open file in
     */
    private void openArraysProductFiles(String dir) {
        try {
            productLongStringW = new FileWriter(Paths.get(dir, "longStringProduct.txt").toString());
            productPositionsW = new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "positionsProduct.bin").toString()));
            productSizesW = new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "sizesProduct.bin").toString()));
            productInfoBlocksW = new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "infoBlocksProduct.bin").toString()));
            locationsLongStringW = new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "locationsLongString.bin").toString()));
        } catch (Exception e) {
            System.out.println("Error creating file in ProductIdDictionary!!!");
        }
    }

    /***
     * Opens all the tokens dictionary files
     * @param dir - The directory in which to open file in
     */
    private void constructorDic(String dir) {
        try {
            frequenciesW = new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "frequencies.bin").toString()));
            postingW = new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "postingLists.bin").toString()));
            productPostingW = new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "productPosting.bin").toString()));
        } catch (Exception e) {
            System.out.println("Error creating file in dictionary!!!");
        }
    }

    /***
     * Opens all the tokens dictionary files
     * @param dir - The directory in which to open file in
     */
    private void openArraysFiles(String dir) {
        try {
            longStringW = new FileWriter(Paths.get(dir, "longString.txt").toString());
            positionsW = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "positions.bin").toString())));
            sizesW = new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "sizes.bin").toString()));
            infoBlocksW = new BufferedOutputStream(new FileOutputStream(Paths.get(dir,
                    "infoBlocks.bin").toString()));
            infoBlocksW.write(toBytes(oppositeTokensIdDict.size(), bytesToBytes));
            infoBlocksW.write(toBytes(reviews, bytesToBytes));
            infoBlocksW.write(toBytes(collection, bytesToBytes));
        } catch (Exception e) {
            System.out.println("Error creating file in dictionary!!!");
        }
    }

    /***
     * Convert number into it's bytes array representation
     * @param i - The number to convert to bytes
     * @return - bytes array representing the given number
     */
    public byte[] intToByteArray(final int i) {
        BigInteger number = BigInteger.valueOf(i);
        byte[] b = number.toByteArray(); // len of array == 2
        byte[] addElem = new byte[2];
        if (b.length == 1) {
            addElem[1] = b[0];
            return addElem;
        }
        return b;
    }

    /***
     * Writes into the temp dictionary the tokens
     * @param line_words - The current review text
     * @param reviewId - The number of The current review
     */
    private int writeTokens(List<String> line_words, int reviewId, int tokensCounter) {
        for (String word : line_words) {
            int termId = tokensIdDict.get(word);
            List<Integer> listD = new ArrayList<>();
            List<Integer> list = tokensTempDict.getOrDefault(termId, listD);
            list.add(reviewId);
            tokensTempDict.put(termId, list);
            tokensCounter += 2;
        }
        line_words.clear();
        return tokensCounter;
    }

    /***
     * Writes into the temp dictionary the products
     * @param product - The current productId
     * @param reviewId - The number of The current review
     */
    private void writeProducts(String product, int reviewId) {
        int termId = productsIdDict.get(product);
        List<Integer> listD = new ArrayList<>();
        List<Integer> list = productsTempDict.getOrDefault(termId, listD);
        list.add(reviewId);
        productsTempDict.put(termId, list);
    }


    /***
     * Writes into the dictionary files data about a given review text
     * @param line - The current review text
     */
    private void createTermIdDictionary(String line) {
        String[] tokens = line.split("[^a-z0-9]+");
        for (int i = 0; i < tokens.length; i++) {
            if (!tokens[i].isEmpty()) {
                tokensSet.add(tokens[i]);
                collection++;
            }
        }
    }

    private void countTokens(String line) {
        String[] tokens = line.split("[^a-z0-9]+");
        for (int i = 0; i < tokens.length; i++) {
            if (!tokens[i].isEmpty()) {
                line_words.add(tokens[i]);
            }
        }
    }


    /***
     * Calculates the mutual prefix of two given strings
     * @param prevToken - First string
     * @param token - Second String
     * @return The mutual prefix of two given strings
     */
    private int findPrefixSize(String prevToken, String token) {
        int size = Math.min(prevToken.length(), token.length());
        int sizePrefix = 0;
        for (int i = 0; i < size; i++) {
            if (prevToken.charAt(i) == token.charAt(i)) {
                sizePrefix++;
            } else break;
        }
        return sizePrefix;
    }

    /***
     * Writes all the products id and their data into the corresponding files.
     */
    private void buildDicProductFile(String dir) {
        int concatenationLength = 0;
        int posAccordingToBlocks = 0; // index to productID
        int positionId = 0;
        boolean first_time = true;
        int termId = 0;
        int nextTermId = 0;
        String prevProduct = "";
        try {
            int positions = 0;
            productInfoBlocksW.write(toBytes(oppositeProductsIdDict.size(), bytesToBytes));
            String path = Paths.get(dir, "sortedFile.bin").toString();
            BufferedInputStream file = new BufferedInputStream(new FileInputStream(path));
            LinkedHashSet<Integer> reviewIds = new LinkedHashSet<>();
            while (file.available() > 0) {
                if (first_time) {
                    file.read(bytesToBytes, 0, 4);
                    termId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                    first_time = false;
                }
                file.read(bytesToBytes, 0, 4);
                int reviewId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                reviewIds.add(reviewId);
                if (file.available() > 0) {
                    file.read(bytesToBytes, 0, 4);
                    nextTermId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                    while ((termId == nextTermId)) {
                        file.read(bytesToBytes, 0, 4);
                        reviewId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                        reviewIds.add(reviewId);
                        if (file.available() > 0) {
                            file.read(bytesToBytes, 0, 4);
                            nextTermId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                        }
                    }
                }
                String product = getTokenById(termId, false);
                productSizesW.write(toBytes(product.length(), bytesToBytes));
                // not new block
                int k_products = 6;
                if (posAccordingToBlocks % k_products != 0) {
                    int curPrefixSize = findPrefixSize(prevProduct, product);
                    productSizesW.write(toBytes(curPrefixSize, bytesToBytes));
                    String productTemp = product.substring(curPrefixSize);
                    productLongStringW.write(productTemp);
                    concatenationLength += productTemp.length();
                    locationsLongStringW.write(toBytes(positions, bytesToBytes));
                    positions += productTemp.length();
                } else {
                    productSizesW.write(toBytes(0, bytesToBytes));
                    productInfoBlocksW.write(toBytes(concatenationLength, bytesToBytes));
                    productLongStringW.write(product);
                    concatenationLength += product.length();
                    locationsLongStringW.write(toBytes(positions, bytesToBytes));
                    positions += product.length();
                }
                int prevReviewId = 0;
                productPositionsW.write(toBytes(positionId, bytesToBytes));
                for (int id : reviewIds) {
                    int gapReviewId = id - prevReviewId;
                    byte[] reviewEncoded = gamma.encode(gapReviewId);
                    positionId += reviewEncoded.length;
                    productPostingW.write(reviewEncoded);
                    prevReviewId = id;
                }
                posAccordingToBlocks++;
                prevProduct = product;
                termId = nextTermId;
                reviewIds.clear();
            }
            file.close();
            productLongStringW.close();
            productInfoBlocksW.close();
            locationsLongStringW.close();
        } catch (Exception e) {
            System.out.println("Error in writing product!");
        }
    }

    /***
     * Writes all the tokens and their data into the corresponding files.
     */
    private void buildDicFile(String dir) {
        long positionId = 0;
        int concatenationLength = 0, posAccordingToBlocks = 0; // index to token number
        String prevToken = "";
        int prevReviewId = 0;
        try {
            String path = Paths.get(dir, "sortedFile.bin").toString();
            BufferedInputStream file = new BufferedInputStream(new FileInputStream(path));
            int allFrequencyInReviews = 0; // how many times token appears in some review id
            file.read(bytesToBytes, 0, 4);
            int prevTermId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            file.read(bytesToBytes, 0, 4);
            int reviewId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            file.read(bytesToBytes, 0, 4);
            int freq = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            int termId = prevTermId;
            int temp = 0;
            positionsW.writeLong(positionId);
            file.read(bytesToBytes, 0, 4);
            temp = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
            int k_words = 24;
            while ((termId != -1) && (temp != -1)) {
                boolean firstTime = true;
                while (termId == prevTermId) {
                    int gapReviewId = reviewId - prevReviewId;
                    byte[] reviewEncoded = gamma.encode(gapReviewId);
                    positionId += reviewEncoded.length;
                    byte[] fregEncoded = gamma.encode(freq);
                    positionId += fregEncoded.length;
                    postingW.write(reviewEncoded);
                    postingW.write(fregEncoded);
                    allFrequencyInReviews += freq;
                    prevReviewId = reviewId;
                    if (firstTime) {
                        termId = temp;
                        firstTime = false;
                    } else {
                        file.read(bytesToBytes, 0, 4);
                        termId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                    }
                    if (termId != -1) {
                        file.read(bytesToBytes, 0, 4);
                        reviewId = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                        file.read(bytesToBytes, 0, 4);
                        freq = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                    }
                }
                prevReviewId = 0;
                String token = getTokenById(prevTermId, true);
                sizesW.write(toBytes(token.length(), bytesToBytes));
                int curPrefixSize = findPrefixSize(prevToken, token);
                sizesW.write(toBytes(curPrefixSize, bytesToBytes));
                // not new block
                if (posAccordingToBlocks % k_words != 0) {
                    String tokenTemp = token.substring(curPrefixSize);
                    longStringW.write(tokenTemp);
                    concatenationLength += tokenTemp.length();
                } else {
                    infoBlocksW.write(toBytes(concatenationLength, bytesToBytes));
                    longStringW.write(token);
                    concatenationLength += token.length();
                }
                byte[] allFreqsEncode = gamma.encode(allFrequencyInReviews);
                frequenciesW.write(allFreqsEncode);
                allFrequencyInReviews = 0;
                posAccordingToBlocks++;
                prevToken = token;
                prevTermId = termId;
                positionsW.writeLong(positionId);
                if (termId != -1) {
                    file.read(bytesToBytes, 0, 4);
                    temp = java.nio.ByteBuffer.wrap(bytesToBytes).getInt();
                }
            }
            if (termId != -1) {
                byte[] reviewEncoded = gamma.encode(reviewId);
                byte[] fregEncoded = gamma.encode(freq);
                postingW.write(reviewEncoded);
                postingW.write(fregEncoded);
                String token = getTokenById(termId, true);
                sizesW.write(toBytes(token.length(), bytesToBytes));
                int curPrefixSize = findPrefixSize(prevToken, token);
                sizesW.write(toBytes(curPrefixSize, bytesToBytes));
                // not new block
                if (posAccordingToBlocks % k_words != 0) {
                    String tokenTemp = token.substring(curPrefixSize);
                    longStringW.write(tokenTemp);
                } else {
                    infoBlocksW.write(toBytes(concatenationLength, bytesToBytes));
                    longStringW.write(token);
                }
                byte[] allFreqsEncode = gamma.encode(freq);
                frequenciesW.write(allFreqsEncode);
            }
            file.close();
            longStringW.close();
        } catch (Exception e) {
            System.out.println("Error in writing!:(");
        }
    }

    private String getTokenById(int termId, boolean isTokens) {
        return isTokens ? oppositeTokensIdDict.get(termId) : oppositeProductsIdDict.get(termId);
    }


    /**
     * Delete all index files by removing the given directory
     */
    public void removeIndex(String dir) {
        try {
            File folder = new File(dir);
            String[] entries = folder.list();
            for (String s : entries) {
                File currentFile = new File(folder.getPath(), s);
                currentFile.delete();
            }
            Files.delete(Paths.get(dir));
        } catch (Exception e) {
            System.out.println("Error Deleting!!!");
        }
    }
}