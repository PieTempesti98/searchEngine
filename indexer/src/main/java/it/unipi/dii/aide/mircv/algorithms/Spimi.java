package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.beans.DocumentIndexEntry;
import it.unipi.dii.aide.mircv.common.beans.PostingList;
import com.google.common.annotations.VisibleForTesting;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;
import it.unipi.dii.aide.mircv.common.utils.CollectionStatistics;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.util.*;
import java.util.stream.Collectors;


public class Spimi {

    /**
     * path to the file on the disk storing the processed collection
     */
    private static final String PATH_TO_DOCUMENTS = ConfigurationParameters.getProcessedCollectionPath();

    /*
    path to the file on the disk storing the partial indexes
     */
    private static final String PATH_PARTIAL_INDEX = ConfigurationParameters.getPartialIndexPath();

    /*
    path to the file on the disk storing the document index
     */
    private static final String PATH_TO_DOCUMENT_INDEX = ConfigurationParameters.getDocumentIndexPath();

    /*
    counts the number of partial indexes created
     */
    private static int numIndex = 0;


    /**
     *
     * @param index: partial index that must be saved onto file
     */
    private static void saveIndexToDisk(HashMap<String, PostingList> index, DB db){

        if(index.isEmpty()) //if the index is empty there is nothing to write on disk
            return;

        //sort index in lexicographic order
        index = index.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));


        //write index to disk
        //try(DB db = DBMaker.fileDB(PATH_BASE_TO_INDEX + num_index + ".db").fileChannelEnable().fileMmapEnable().make()){
        List<PostingList> partialIndex = (List<PostingList>) db.indexTreeList("index_" + numIndex, Serializer.JAVA).createOrOpen();
        partialIndex.addAll(index.values());

        //update number of partial inverted indexes
        numIndex++;

    }

    /**
     * @param docid: docid of a certain document
     * @param postingList: posting list of a given term
     *
     *                   Function that searched for a given docid in a posting list.
     *                   If the document is already present it updates the term frequency for that
     *                   specific document, if that's not the case creates a new pair (docid,freq)
     *                   in which frequency is set to 1 and adds this pair to the posting list
     * **/
    private static void updateOrAddPosting(int docid, PostingList postingList){
        if(postingList.getPostings().size() > 0) {
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
        postingList.getPostings().add(new AbstractMap.SimpleEntry<>(docid,1));

    }

    /*
    *  performs Spimi algorithm
    * */

    public static int executeSpimi(){

        try(DB db = DBMaker.fileDB(PATH_TO_DOCUMENTS).fileChannelEnable().fileMmapEnable().make(); //fileDb for  processed documents
            DB partialIndex = DBMaker.fileDB(PATH_PARTIAL_INDEX).fileChannelEnable().fileMmapEnable().make();
            DB docIndexDb = DBMaker.fileDB(PATH_TO_DOCUMENT_INDEX).fileChannelEnable().fileMmapEnable().make(); //fileDB for document index
            HTreeMap collection = db.hashMap("processedCollection")//hashmap containing processd collection
                    .keySerializer(Serializer.STRING) //key ->pid
                    .valueSerializer(Serializer.JAVA) // value -> posting list
                    .createOrOpen();
        ){
            boolean allDocumentsProcessed = false; //is set to true when all documents are read

            //list containing all documents indexes that must be written on file
            Map<Integer,DocumentIndexEntry> docIndex= (Map<Integer,DocumentIndexEntry>)docIndexDb.hashMap("docIndex")
                    .keySerializer(Serializer.INTEGER) //key-> docid
                    .valueSerializer(Serializer.JAVA) //value -> document info
                    .createOrOpen();

            int docid = 0; //assign docid in a incremental manner

            long MEMORY_THRESHOLD = Runtime.getRuntime().totalMemory() * 20 / 100; // leave 20% of memory free
            Iterator<String> keyterator = (Iterator<String>) collection.keySet().iterator();

            while(!allDocumentsProcessed) {
                HashMap<String, PostingList> index = new HashMap<>(); //hashmap containing partial index
                while (Runtime.getRuntime().freeMemory() > MEMORY_THRESHOLD) { //build index until 80% of total memory is used

                    if(!keyterator.hasNext()){ // all documents were processed
                        allDocumentsProcessed = true;
                        break;
                    }
                    //parse line to get pid and all terms of a document
                    ProcessedDocument document = new ProcessedDocument();
                    String pid = keyterator.next(); //the pid is the key of the hashmap
                    document.setPid(pid);
                    document.setTokens((ArrayList<String>) collection.get(pid));


                    //create new document index entry and add it to file
                    DocumentIndexEntry entry = new DocumentIndexEntry(pid,docid++,document.getTokens().size());
                    docIndex.put(docid,entry);

                    CollectionStatistics.addDocument(); //keeps track of number of processed documents,
                                                        // useful for calculating collection statistics later on

                    for (String term : document.getTokens()) {
                        PostingList posting; //posting list of a given term
                        if (!index.containsKey(term)) {
                           // create new posting list if term wasn't present yet
                            posting = new PostingList(term);
                            index.put(term, posting); //add new entry (term, posting list) to entry
                        } else {
                            //term is present, we can get its posting list
                            posting = index.get(term);
                        }

                        updateOrAddPosting(docid, posting); //insert or update new posting
                    }

                }
                saveIndexToDisk(index,partialIndex);  //either if there is no  memory available or all documents were read, flush partial index onto disk
            }
            return numIndex;

        }catch (Exception e){
            e.printStackTrace();
            return 0;
        }
    }


}
