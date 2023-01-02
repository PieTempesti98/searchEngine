package it.unipi.dii.aide.mircv.cacheTests;

import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;
import it.unipi.dii.aide.mircv.common.beans.TextDocument;
import it.unipi.dii.aide.mircv.common.beans.Vocabulary;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;
import queryProcessing.DAAT;
import queryProcessing.MaxScore;
import queryProcessing.QueryProcesser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.PriorityQueue;

public class CacheTests {

    private static Vocabulary vocabulary = Vocabulary.getInstance();
    private static boolean maxScore = true;
    private static int k = 100;
    private static final String QUERIES_PATH = "data/queries/queries.txt";
    private static final String RESULT_PATH = "data/queries/results.txt";
    private static final String STAT_PATH = "data/queries/stats.txt";
    private static String SCORING_FUNCTION = "tfidf";
    private static long timeNoCache = 0;
    private static long timeCache = 0;
    private static long totQueries = 0;

    private static void processQueries() {


        try (
                BufferedReader br = Files.newBufferedReader(Paths.get(QUERIES_PATH), StandardCharsets.UTF_8);
                BufferedWriter resultBuffer = new BufferedWriter(new FileWriter(RESULT_PATH, true));

        ) {
            System.out.println("Starting processing queries");

            String line;


            while ((line = br.readLine()) != null) {

                // if the line is empty we process the next line
                if (line.isBlank())
                    continue;

                // split of the line in the format <qid>\t<text>
                String[] split = line.split("\t");

                if (split.length != 2)
                    continue;

                // Creation of the text document for the line
                TextDocument document = new TextDocument(split[0], split[1].replaceAll("[^\\x00-\\x7F]", ""));
                // Perform text preprocessing on the document
                ProcessedDocument processedQuery = Preprocesser.processDocument(document);

                // load the posting lists of the tokens


                PriorityQueue<Map.Entry<Double, Integer>> priorityQueue;

                // score query with cache
                long start = System.currentTimeMillis();
                ArrayList<PostingList> queryPostings = QueryProcesser.getQueryPostings(processedQuery, false);
                if (queryPostings == null || queryPostings.isEmpty()) {
                    continue;
                }
                if (!maxScore)
                    priorityQueue = DAAT.scoreQuery(queryPostings, false, k, SCORING_FUNCTION);
                else
                    priorityQueue = MaxScore.scoreQuery(queryPostings, k, SCORING_FUNCTION, false);

                long stop = System.currentTimeMillis();

                if (priorityQueue.isEmpty())
                    continue;

                long responseTime = stop - start;

                System.out.println("response time for query " + processedQuery.getPid() + "is: " + responseTime + " milliseconds");

                resultBuffer.write(processedQuery.getPid() + '\t' + responseTime + '\t' + " no cache " + '\n');

                timeNoCache += responseTime;


                // repeat scoring of same query with cache

                start = System.currentTimeMillis();
                queryPostings = QueryProcesser.getQueryPostings(processedQuery, false);
                if (queryPostings == null || queryPostings.isEmpty()) {
                    continue;
                }
                if (!maxScore)
                    DAAT.scoreQuery(queryPostings, false, k, SCORING_FUNCTION);
                else
                    MaxScore.scoreQuery(queryPostings, k, SCORING_FUNCTION, false);

                stop = System.currentTimeMillis();

                responseTime = stop - start;

                System.out.println("response time for query " + processedQuery.getPid() + "is: " + responseTime + " milliseconds");


                resultBuffer.write('\n');
                resultBuffer.write(processedQuery.getPid() + '\t' + responseTime + '\t' + " cache " + '\n');

                timeCache += responseTime;

                Vocabulary.clearCache();

                totQueries++;


            }

        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    public static void main(String[] args) throws IOException {

        System.out.println("Setting up...");

        boolean setupSuccess = QueryProcesser.setupProcesser();

        if (!setupSuccess) {
            System.out.println("Error in setting up of the service");
            return;
        }

        System.out.println("Starting tests");


        processQueries();

        SCORING_FUNCTION = "bm25";

        processQueries();

        try (
                BufferedWriter statBuffer = new BufferedWriter(new FileWriter(STAT_PATH, true));
        ) {

            double avgNoCache = (double) timeNoCache / (double) totQueries;

            double avgCache = (double) timeCache / (double) totQueries;

            statBuffer.write("avg time without cache: " + '\t' + avgNoCache);
            statBuffer.write('\n');
            statBuffer.write("avg time with cache: " + '\t' + avgCache);


        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
