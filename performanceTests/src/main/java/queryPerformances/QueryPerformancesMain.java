package queryPerformances;

import it.unipi.dii.aide.mircv.common.beans.DocumentIndex;
import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;
import it.unipi.dii.aide.mircv.common.beans.TextDocument;
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


public class QueryPerformancesMain {
    /**
     * integer defining the top documents to return
     */
    private static final int k = 100;
    private static final String SCORING_FUNCTION = "bm25";
    private static final String QUERIES_PATH = "data/queries/queries.txt";
    private static final String TREC_EVAL_RESULTS_PATH = "data/queries/search_engine_results_" + SCORING_FUNCTION + ".txt";
    private static final boolean maxScore = false;
    private static final boolean isTrecEvalTest = false;
    private static final String fixed = "Q0";
    private static final String runid = "RUN-01";

    private static boolean saveResultsForTrecEval(String topicId, PriorityQueue<Map.Entry<Double, Integer>> priorityQueue) {
        int i = priorityQueue.size();
        DocumentIndex documentIndex = DocumentIndex.getInstance();

        try (
                BufferedWriter statisticsBuffer = new BufferedWriter(new FileWriter(TREC_EVAL_RESULTS_PATH, true))
        ) {
            String resultsLine;

            while (priorityQueue.peek() != null) {
                Map.Entry<Double, Integer> resEntry = priorityQueue.poll();
                resultsLine = topicId + "\t" + fixed + "\t" + documentIndex.getPid(resEntry.getValue()) + "\t" + i + "\t" + resEntry.getKey() + "\t" + runid + "\n";
                statisticsBuffer.write(resultsLine);
                i--;
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    public static void main(String[] args){

        System.out.println("Starting...");
        //check is set up of data structures was successful
        boolean setupSuccess = QueryProcesser.setupProcesser();

        if(!setupSuccess){
            System.out.println("Error in setup of this service.");
            return;
        }

        try (
                BufferedReader br = Files.newBufferedReader(Paths.get(QUERIES_PATH), StandardCharsets.UTF_8)
        ){
            System.out.println("Starting processing queries");

            String line;
            long sumResponseTime = 0;
            int nQueries = 0;
            ArrayList<Long> responseTimes = new ArrayList<>();

            while(true){
                // if we reach the end of file (br.readline() -> null)
                if ((line = br.readLine()) == null) {
                    System.out.println("all queries processed");
                    break;
                }
                // if the line is empty we process the next line
                if (line.isBlank())
                    continue;

                // split of the line in the format <qid>\t<text>
                String[] split = line.split("\t");

                if(split.length != 2)
                    continue;

                // Creation of the text document for the line
                TextDocument document = new TextDocument(split[0], split[1].replaceAll("[^\\x00-\\x7F]", ""));
                // Perform text preprocessing on the document
                ProcessedDocument processedQuery = Preprocesser.processDocument(document);

                // load the posting lists of the tokens
                ArrayList<PostingList> queryPostings = QueryProcesser.getQueryPostings(processedQuery,false);
                if(queryPostings == null || queryPostings.isEmpty()){
                    continue;
                }

                PriorityQueue<Map.Entry<Double, Integer>> priorityQueue;

                long start = System.currentTimeMillis();
                if (!maxScore)
                    priorityQueue = DAAT.scoreQuery(queryPostings, false, k, SCORING_FUNCTION);
                else
                    priorityQueue = MaxScore.scoreQuery(queryPostings, k, SCORING_FUNCTION, false);
                long stop = System.currentTimeMillis();

//                System.out.println("response time for query "+ processedQuery.getPid() + " is: "+(stop-start)+" milliseconds");

                nQueries++;
                sumResponseTime += (stop - start);
                responseTimes.add(stop - start);


                if (isTrecEvalTest)
                    if (!saveResultsForTrecEval(processedQuery.getPid(), priorityQueue))
                        System.out.println("Error encountered while writing trec_eval_results");

            }

            double mean = sumResponseTime / (double) nQueries;
            double standardDeviation = 0.0;
            for (long num : responseTimes) {
                standardDeviation += Math.pow(num - mean, 2);
            }
            standardDeviation = Math.sqrt(standardDeviation / nQueries);
            System.out.println("mean query response time is: " + sumResponseTime / nQueries + " milliseconds, with a std dev of " + standardDeviation);

        } catch (IOException e) {
            System.out.println("tests failed");
            throw new RuntimeException(e);
        }
    }
}
