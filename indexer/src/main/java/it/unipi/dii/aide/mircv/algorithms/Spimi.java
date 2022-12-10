package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.beans.*;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;
import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class Spimi {

    /**
     * path to the file on the disk storing the processed collection
     */
    private static final String PATH_TO_COLLECTION = ConfigurationParameters.getRawCollectionPath();

    /*
    path to the file on the disk storing the partial indexes
     */
    private static final String PATH_PARTIAL_INDEX = ConfigurationParameters.getPartialIndexPath();

    /*
    path to the file on the disk storing the document index
     */
    private static final String PATH_TO_DOCUMENT_INDEX = ConfigurationParameters.getDocumentIndexPath();

    /*
    path to the file on the disk storing the vocabulary
    */
    private static final String PATH_TO_VOCABULARY =  "data/partial_vocabulary/vocabulary";

    /*
    counts the number of partial indexes created
     */
    private static int numIndex = 0;


    /**
     * @param index: partial index that must be saved onto file
     */
    private static void saveIndexToDisk(HashMap<String, PostingList> index, int numDocs) {

        if (index.isEmpty()) //if the index is empty there is nothing to write on disk
            return;

        //sort index in lexicographic order
        index = index.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));


        long startPosition = 0;

        System.out.println("********* START VOCABULARY **********");


        for(PostingList postingList: index.values()) {
            //postingList.write_to_disk();
            long postingsSize = postingList.getNumBytes();
            VocabularyEntry entry = new VocabularyEntry(postingList.getTerm());
            //compute entry statistics
            entry.updateStatistics(postingList);
            entry.computeIDF(numDocs);

            //set size of memory offset

            //set size of frequency offset

            //set size of posting list
            entry.setMemorySize(postingsSize);
            //create a new file for each partial vocabulary and update starting position for writing to disk
            startPosition += entry.write_entry_to_disk(startPosition,PATH_TO_VOCABULARY+"_"+numIndex);


        }


        //update number of partial inverted indexes
        numIndex++;

    }

    /**
     * @param docid:       docid of a certain document
     * @param postingList: posting list of a given term
     *                     <p>
     *                     Function that searched for a given docid in a posting list.
     *                     If the document is already present it updates the term frequency for that
     *                     specific document, if that's not the case creates a new pair (docid,freq)
     *                     in which frequency is set to 1 and adds this pair to the posting list
     **/
    private static void updateOrAddPosting(int docid, PostingList postingList) {
        if (postingList.getPostings().size() > 0) {
            // last document inserted:
            Map.Entry<Integer, Integer> posting = postingList.getPostings().get(postingList.getPostings().size() - 1);
            //If the docId is the same I update the posting
            if (docid == posting.getKey()) {
                posting.setValue(posting.getValue() + 1);
                return;
            }
        }
        //the document has not been processed (docIds are incremental):
        // create new pair and add it to the posting list
        postingList.getPostings().add(new AbstractMap.SimpleEntry<>(docid, 1));

    }


    /**
     * Performs spimi algorithm
     *
     * @return the number of partial indexes created
     */
    public static int executeSpimi() {

        try (
                BufferedReader br = Files.newBufferedReader(Paths.get(PATH_TO_COLLECTION), StandardCharsets.UTF_8);
        ) {
            boolean allDocumentsProcessed = false; //is set to true when all documents are read

            int docid = 0; //assign docid in a incremental manner
            int partialNumDocs = 0;

            long MEMORY_THRESHOLD = Runtime.getRuntime().totalMemory() * 20 / 100; // leave 20% of memory free
            String[] split;
            while (!allDocumentsProcessed) {
                HashMap<String, PostingList> index = new HashMap<>(); //hashmap containing partial index
                while (Runtime.getRuntime().freeMemory() > MEMORY_THRESHOLD) { //build index until 80% of total memory is used

                    String line;
                    // if we reach the end of file (br.readline() -> null)
                    if ((line = br.readLine()) == null) {
                        // we've processed all the documents
                        allDocumentsProcessed = true;
                        break;
                    }
                    // if the line is empty we process the next line
                    if (line.isEmpty())
                        continue;

                    // split of the line in the format <pid>\t<text>
                    split = line.split("\t");

                    // Creation of the text document for the line
                    TextDocument document = new TextDocument(split[0], split[1].replaceAll("[^\\x00-\\x7F]", ""));
                    // Perform text preprocessing on the document
                    ProcessedDocument processedDocument = Preprocesser.processDocument(document);

                    if (processedDocument.getTokens().isEmpty()) {
                        continue;
                    }

                    //create new document index entry and add it to file
                   DocumentIndexEntry entry = new DocumentIndexEntry(
                            processedDocument.getPid(),
                            docid++,
                            processedDocument.getTokens().size()
                    );

                   // entry.write_to_disk();

                    //keeps track of number of processed documents,
                    // useful for calculating collection statistics later on
                    CollectionStatistics.addDocument();
                    partialNumDocs ++;

                    for (String term : processedDocument.getTokens()) {

                        if(term.length() == 0)
                            continue;

                        PostingList posting; //posting list of a given term
                        if (!index.containsKey(term)) {
                            // create new posting list if term wasn't present yet
                            posting = new PostingList(term);
                            index.put(term, posting); //add new entry (term, posting list) to entry
                        } else {
                            //term is present, we can get its posting list
                            posting = index.get(term);
                        }

                        //insert or update new posting
                        updateOrAddPosting(docid, posting);

                    }



                }
                //either if there is no  memory available or all documents were read, flush partial index onto disk
                saveIndexToDisk(index,partialNumDocs);
                partialNumDocs = 0;
                System.out.println("salvo");
            }
            return numIndex;

        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }


    public static void main(String[] args){
        Spimi.executeSpimi();

        VocabularyEntry entry = new VocabularyEntry();
        long iter = 0;

        while(entry.readFromDisk(iter * entry.getENTRY_SIZE())) {
            System.out.println(entry);
            iter ++;
        }
    }


}
