package it.unipi.dii.aide.mircv.algorithms.test;

import it.unipi.dii.aide.mircv.algorithms.Merger;
import it.unipi.dii.aide.mircv.common.beans.*;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.config.Flags;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

import static it.unipi.dii.aide.mircv.utils.Utility.initializeFiles;

public class MergerTest {

    /*
    path to the file on the disk storing the partial vocabulary
    */
    private static final String PATH_TO_PARTIAL_VOCABULARY = ConfigurationParameters.getPartialVocabularyDir() + ConfigurationParameters.getVocabularyFileName();

    /*
    path to the file on the disk storing the partial frequencies of the posting list
    */
    private static final String PATH_TO_PARTIAL_FREQUENCIES = ConfigurationParameters.getFrequencyDir() + ConfigurationParameters.getFrequencyFileName();

    /*
    path to the file on the disk storing the partial docids of the posting list
    */
    private static final String PATH_TO_PARTIAL_DOCID = ConfigurationParameters.getDocidsDir() + ConfigurationParameters.getDocidsFileName();

    private static boolean verboseMode;

    private static void printIndex(ArrayList<ArrayList<Posting>> index){
        for(ArrayList<Posting> postingList: index){
            System.out.println("\t- posting list:");
            for(Posting posting: postingList){
                System.out.println("\t\t- posting: ("+ posting.getDocid()+","+posting.getFrequency()+")");
            }
        }
    }

    private static boolean writeIntermediateIndexesToDisk(ArrayList<ArrayList<PostingList>> intermediateIndexes) {
        for (ArrayList<PostingList> intermediateIndex : intermediateIndexes) {

            int i = intermediateIndexes.indexOf(intermediateIndex);

            if(verboseMode)
                System.out.println("writing index "+i+" to disk:");

            try (
                    FileChannel docsFchan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_PARTIAL_DOCID + "_" + i),
                            StandardOpenOption.WRITE,
                            StandardOpenOption.READ,
                            StandardOpenOption.CREATE
                    );
                    FileChannel freqsFchan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_PARTIAL_FREQUENCIES + "_" + i),
                            StandardOpenOption.WRITE,
                            StandardOpenOption.READ,
                            StandardOpenOption.CREATE);
                    FileChannel vocabularyFchan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_PARTIAL_VOCABULARY + "_" + i),
                            StandardOpenOption.WRITE,
                            StandardOpenOption.READ,
                            StandardOpenOption.CREATE)
            ) {
                long vocOffset = 0;
                long docidOffset = 0;
                long freqOffset = 0;
                for(PostingList postingList: intermediateIndex){
                    int numPostings = intermediateIndex.size();
                    // instantiation of MappedByteBuffer for integer list of docids and for integer list of freqs
                    MappedByteBuffer docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, docidOffset, numPostings * 4L);
                    MappedByteBuffer freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, freqOffset, numPostings * 4L);

                    // check if MappedByteBuffers are correctly instantiated
                    if (docsBuffer != null && freqsBuffer != null) {
                        //create vocabulary entry
                        VocabularyEntry vocEntry = new VocabularyEntry(postingList.getTerm());
                        vocEntry.setMemoryOffset(docsBuffer.position());
                        vocEntry.setFrequencyOffset(docsBuffer.position());

                        // write postings to file
                        for (Posting posting : postingList.getPostings()) {
                            if(verboseMode)
                                System.out.println("\t- writing posting: " + "("+posting.getDocid()+","+posting.getFrequency()+")");

                            // encode docid and freq
                            docsBuffer.putInt(posting.getDocid());
                            freqsBuffer.putInt(posting.getFrequency());
                        }

                        vocEntry.updateStatistics(postingList);
                        vocEntry.setDocidSize(numPostings * 4);
                        vocEntry.setFrequencySize(numPostings * 4);
                        vocEntry.setMemoryOffset(docidOffset);
                        vocEntry.setFrequencyOffset(freqOffset);

                        if(verboseMode)
                            System.out.println("\t- writing vocabulary entry: " + "{"+vocEntry+"}");

                        vocOffset = vocEntry.writeEntryToDisk(vocOffset, vocabularyFchan);

                        docidOffset+=numPostings*4L;
                        freqOffset+=numPostings*4L;

                    }
                    else
                        return false;
                }
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    private static ArrayList<ArrayList<Posting>> retrieveIndexFromDisk(){

        if(verboseMode)
            System.out.println("\nRetrieving merging results\n");

        // get vocabulary from disk
        Vocabulary v = Vocabulary.getInstance();
        v.readFromDisk();

        ArrayList<ArrayList<Posting>> mergedLists = new ArrayList<>(v.size());

        ArrayList<VocabularyEntry> vocEntries = new ArrayList<>();
        vocEntries.addAll(v.values());

        for(VocabularyEntry vocabularyEntry: vocEntries){
            if(verboseMode){
                System.out.println("vocabulary entry for term: "+vocabularyEntry.getTerm());
                System.out.println("\t"+vocabularyEntry);
            }

            PostingList p = new PostingList();
            p.setTerm(vocabularyEntry.getTerm());
            p.openList();
            ArrayList<Posting> postings = new ArrayList<>();

            while(p.next()!=null){
                if(verboseMode)
                    System.out.println("retrieved posting: ("+p.getCurrentPosting().getDocid()+","+p.getCurrentPosting().getFrequency()+")");
                postings.add(p.getCurrentPosting());
            }

            p.closeList();

            mergedLists.add(postings);
        }

        return mergedLists;
    }

    private static boolean checkResults(ArrayList<ArrayList<Posting>> mergedLists, ArrayList<ArrayList<Posting>> expectedResults){

        if(expectedResults.size() != mergedLists.size())
            return false;

        for(ArrayList<Posting> postingList: mergedLists) {
            ArrayList<Posting> expectedPostingList = expectedResults.get(mergedLists.indexOf(postingList));
            if(expectedPostingList == null || expectedPostingList.size() != postingList.size())
                return false;

            for (Posting p : postingList) {
                Posting expectedPosting = expectedPostingList.get(postingList.indexOf(p));
                if(expectedPosting == null)
                    return false;

                if (p.getDocid() != expectedPosting.getDocid() || p.getFrequency() != expectedPosting.getFrequency())
                    return false;
            }
        }
        return true;
    }

    private static boolean test1(boolean compressionMode){
        System.out.println("\n----------- TEST 1 ------------\n");

        System.out.println("""
                \tThe aim of this test is to apply merger with just one intermediate index.
                \tThe result should be an index equal to the input one
                """);

        // building partial index 1
        ArrayList<PostingList> index1 = new ArrayList<>();

        index1.add(new PostingList("alberobello\t1:3 2:4: 3:7"));
        index1.add(new PostingList("newyork\t1:5 3:2: 4:6"));
        index1.add(new PostingList("pisa\t2:1 5:3"));

        // insert partial index to array of partial indexes
        ArrayList<ArrayList<PostingList>> intermediateIndexes = new ArrayList<>();
        intermediateIndexes.add(index1);

        // write intermediate indexes to disk so that SPIMI can be executed
        if (!writeIntermediateIndexesToDisk(intermediateIndexes)) {
            System.out.println("\nError while writing intermediate indexes to disk\n");
            return false;
        }

        // merging intermediate indexes
        if (!Merger.mergeIndexes(intermediateIndexes.size(), compressionMode, true)) {
            System.out.println("Error: merging failed\n");
            return false;
        }

        ArrayList<ArrayList<Posting>> mergedLists = retrieveIndexFromDisk();

        // build expected results
        ArrayList<ArrayList<Posting>> expectedResults1 = new ArrayList<>(3);
        ArrayList<Posting> postings1 = new ArrayList<>();

        postings1.add(new Posting(1,3));
        postings1.add(new Posting(2,4));
        postings1.add(new Posting(3,7));
        expectedResults1.add(postings1);
        postings1 = new ArrayList<>();
        postings1.add(new Posting(1,5));
        postings1.add(new Posting(3,2));
        postings1.add(new Posting(4,6));
        expectedResults1.add(postings1);
        postings1 = new ArrayList<>();
        postings1.add(new Posting(2,1));
        postings1.add(new Posting(5,3));
        expectedResults1.add(postings1);

        // check if expected results are equal to actual results
        if(!checkResults(mergedLists, expectedResults1)){
            System.out.println("\nERROR: TEST 1 FAILED\n");
            System.out.println("EXPECTED RESULTS:");
            printIndex(expectedResults1);

            System.out.println("\nACTUAL RESULTS:");
            printIndex(mergedLists);
            return false;
        }

        System.out.println("\nTEST 1 ENDED SUCCESSFULLY\n");
        return true;
    }

    private static boolean test2(boolean compressionMode){
        System.out.println("\n----------- TEST 2 ------------\n");

        System.out.println("""
                \tThe aim of this test is to apply merger with just two simple intermediate indexes.
                """);

        // building partial index 1
        ArrayList<PostingList> index1 = new ArrayList<>();

        index1.add(new PostingList("amburgo\t1:4 2:2: 3:5"));
        index1.add(new PostingList("pisa\t2:1 3:2"));
        index1.add(new PostingList("zurigo\t2:1 3:2"));

        // building partial index 2
        ArrayList<PostingList> index2 = new ArrayList<>();

        index2.add(new PostingList("alberobello\t4:3 5:1"));
        index2.add(new PostingList("pisa\t5:2"));

        // insert partial index to array of partial indexes
        ArrayList<ArrayList<PostingList>> intermediateIndexes = new ArrayList<>();
        intermediateIndexes.add(index1);
        intermediateIndexes.add(index2);

        // write intermediate indexes to disk so that SPIMI can be executed
        if (!writeIntermediateIndexesToDisk(intermediateIndexes)) {
            System.out.println("\nError while writing intermediate indexes to disk\n");
            return false;
        }

        // merging intermediate indexes
        if (!Merger.mergeIndexes(intermediateIndexes.size(), compressionMode, true)) {
            System.out.println("Error: merging failed\n");
            return false;
        }

        ArrayList<ArrayList<Posting>> mergedLists = retrieveIndexFromDisk();

        // build expected results
        ArrayList<ArrayList<Posting>> expectedResults = new ArrayList<>(4);

        index1.add(new PostingList("amburgo\t1:4 2:2: 3:5"));
        index1.add(new PostingList("pisa\t2:1 3:2"));
        index1.add(new PostingList("zurigo\t2:1 3:2"));
        index2.add(new PostingList("alberobello\t4:3 5:1"));
        index2.add(new PostingList("pisa\t5:2"));

        ArrayList<Posting> postings1 = new ArrayList<>();

        postings1.add(new Posting(4,3));
        postings1.add(new Posting(5,1));
        expectedResults.add(postings1);
        postings1 = new ArrayList<>();
        postings1.add(new Posting(1,4));
        postings1.add(new Posting(2,2));
        postings1.add(new Posting(3,5));
        expectedResults.add(postings1);
        postings1 = new ArrayList<>();
        postings1.add(new Posting(2,1));
        postings1.add(new Posting(3,2));
        postings1.add(new Posting(5,2));
        expectedResults.add(postings1);
        postings1 = new ArrayList<>();
        postings1.add(new Posting(2, 1));
        postings1.add(new Posting(3,2));
        expectedResults.add(postings1);


        // check if expected results are equal to actual results
        if(!checkResults(mergedLists, expectedResults)){
            System.out.println("\nERROR: TEST 2 FAILED\n");
            System.out.println("EXPECTED RESULTS:");
            printIndex(expectedResults);

            System.out.println("\nACTUAL RESULTS:");
            printIndex(mergedLists);
            return false;
        }

        System.out.println("\nTEST 2 ENDED SUCCESSFULLY\n");
        return true;
    }


    private static boolean test3(boolean compressionMode) {
        System.out.println("\n----------- TEST 3 ------------\n");

        System.out.println("""
                \tThe aim of this test is to apply merger with just two simple intermediate indexes
                and to test wether if the
                resulting vocabulary is correct or not.
                """);

        // building partial index 1
        ArrayList<PostingList> index1 = new ArrayList<>();

        index1.add(new PostingList("amburgo\t1:4 2:2: 3:5"));
        index1.add(new PostingList("pisa\t2:1 3:2"));
        index1.add(new PostingList("zurigo\t2:1 3:2"));

        // building partial index 2
        ArrayList<PostingList> index2 = new ArrayList<>();

        index2.add(new PostingList("alberobello\t4:3 5:1"));
        index2.add(new PostingList("pisa\t5:2"));

        // insert partial index to array of partial indexes
        ArrayList<ArrayList<PostingList>> intermediateIndexes = new ArrayList<>();
        intermediateIndexes.add(index1);
        intermediateIndexes.add(index2);

        // write intermediate indexes to disk so that SPIMI can be executed
        if (!writeIntermediateIndexesToDisk(intermediateIndexes)) {
            System.out.println("\nError while writing intermediate indexes to disk\n");
            return false;
        }

        // merging intermediate indexes
        if (!Merger.mergeIndexes(intermediateIndexes.size(), compressionMode, true)) {
            System.out.println("Error: merging failed\n");
            return false;
        }

        ArrayList<ArrayList<Posting>> mergedLists = retrieveIndexFromDisk();

        // build expected results
        ArrayList<ArrayList<Posting>> expectedResults = new ArrayList<>(4);

        index1.add(new PostingList("amburgo\t1:4 2:2: 3:5"));
        index1.add(new PostingList("pisa\t2:1 3:2"));
        index1.add(new PostingList("zurigo\t2:1 3:2"));
        index2.add(new PostingList("alberobello\t4:3 5:1"));
        index2.add(new PostingList("pisa\t5:2"));



        ArrayList<Posting> postings1 = new ArrayList<>();

        postings1.add(new Posting(4,3));
        postings1.add(new Posting(5,1));
        expectedResults.add(postings1);
        postings1 = new ArrayList<>();
        postings1.add(new Posting(1,4));
        postings1.add(new Posting(2,2));
        postings1.add(new Posting(3,5));
        expectedResults.add(postings1);
        postings1 = new ArrayList<>();
        postings1.add(new Posting(2,1));
        postings1.add(new Posting(3,2));
        postings1.add(new Posting(5,2));
        expectedResults.add(postings1);
        postings1 = new ArrayList<>();
        postings1.add(new Posting(2, 1));
        postings1.add(new Posting(3,2));
        expectedResults.add(postings1);


        // check if expected results are equal to actual results
        if(!checkResults(mergedLists, expectedResults)){
            System.out.println("\nERROR: TEST 2 FAILED\n");
            System.out.println("EXPECTED RESULTS:");
            printIndex(expectedResults);

            System.out.println("\nACTUAL RESULTS:");
            printIndex(mergedLists);
            return false;
        }

        Vocabulary v = Vocabulary.getInstance();
        v.readFromDisk();

        /*
        for(VocabularyEntry vocabularyEntry: Vocabulary.getInstance().values()){
            System.out.println("vocabulary entry: "+vocabularyEntry);
        }
         */

        // TODO: handle max bm25

/*
        // building expected vocabulary entries
        ArrayList<VocabularyEntry> expectedVocabulary = new ArrayList<>();
        VocabularyEntry vocabularyEntry = new VocabularyEntry("alberobello");
        vocabularyEntry.setDf(2);
        vocabularyEntry.setIdf(Math.log10(5/(double)2));
        vocabularyEntry.setMaxTf(3);
        vocabularyEntry.setMaxDl(2);
        vocabularyEntry.setMaxBM25();
        vocabularyEntry.setMemoryOffset(0);
        vocabularyEntry.setFrequencyOffset(0);
        vocabularyEntry.setDocidSize(0);
        vocabularyEntry.setFrequencySize(0);


        vocabularyEntry.setNumBlocks(1);
        vocabularyEntry.setBlockOffset(0);
        expectedVocabulary.add(vocabularyEntry);

        vocabularyEntry = new VocabularyEntry("amburgo");
        vocabularyEntry.setDf(3);
        vocabularyEntry.setIdf(Math.log10(5/(double)3));
        vocabularyEntry.setMaxTf(5);
        vocabularyEntry.setMaxDl(3);
        vocabularyEntry.setMaxBM25();

        vocabularyEntry.setMemoryOffset(2);
        vocabularyEntry.setFrequencyOffset(2);
        vocabularyEntry.setDocidSize(0);
        vocabularyEntry.setFrequencySize(0);

        vocabularyEntry.setNumBlocks(1);
        vocabularyEntry.setBlockOffset(BlockDescriptor.BLOCK_DESCRIPTOR_ENTRY_BYTES);
        expectedVocabulary.add(vocabularyEntry);

        vocabularyEntry = new VocabularyEntry("pisa");
        vocabularyEntry.setDf(3);
        vocabularyEntry.setIdf(Math.log10(5/(double)3));
        vocabularyEntry.setMaxTf(2);
        vocabularyEntry.setMaxDl(3);
        vocabularyEntry.setMaxBM25();


        vocabularyEntry.setMemoryOffset(5);
        vocabularyEntry.setFrequencyOffset(4);
        vocabularyEntry.setDocidSize(0);
        vocabularyEntry.setFrequencySize(0);

        vocabularyEntry.setNumBlocks(1);
        vocabularyEntry.setBlockOffset(BlockDescriptor.BLOCK_DESCRIPTOR_ENTRY_BYTES*2);
        expectedVocabulary.add(vocabularyEntry);

        vocabularyEntry = new VocabularyEntry("zurigo");
        vocabularyEntry.setDf(2);
        vocabularyEntry.setIdf(Math.log10(5/(double)2));
        vocabularyEntry.setMaxTf(2);
        vocabularyEntry.setMaxDl(3);
        vocabularyEntry.setMaxBM25();

        vocabularyEntry.setMemoryOffset(8);
        vocabularyEntry.setFrequencyOffset(6);
        vocabularyEntry.setDocidSize(0);
        vocabularyEntry.setFrequencySize(0);

        vocabularyEntry.setNumBlocks(1);
        vocabularyEntry.setBlockOffset(BlockDescriptor.BLOCK_DESCRIPTOR_ENTRY_BYTES*3);
        expectedVocabulary.add(vocabularyEntry);

        if(expectedVocabulary.size() != Vocabulary.getInstance().size()){
            System.out.println("\nERROR: TEST 3 FAILED\n");
            System.out.println("\texpected vocabulary size: "+expectedVocabulary.size());
            System.out.println("\tactual vocabulary size: "+Vocabulary.getInstance().size());
        }

        for(VocabularyEntry expectedVocEntry: expectedVocabulary){
            VocabularyEntry actualVocEntry = Vocabulary.getInstance().get(expectedVocEntry.getTerm());
            if(actualVocEntry==null ||
                expectedVocEntry.getDf()!=actualVocEntry.getDf() ||
                expectedVocEntry.getIdf()!=actualVocEntry.getIdf() ||
                expectedVocEntry.getMaxTf()!=actualVocEntry.getMaxTf() ||
                expectedVocEntry.getMaxDl()!=actualVocEntry.getMaxDl() ||
                expectedVocEntry.getMaxBM25()!=actualVocEntry.getMaxBM25() ||
                expectedVocEntry.getDocidOffset()!=actualVocEntry.getDocidOffset() ||
                expectedVocEntry.getFrequencyOffset()!= actualVocEntry.getFrequencyOffset() ||
                expectedVocEntry.getDocidSize()!= actualVocEntry.getDocidSize() ||
                expectedVocEntry.getFrequencySize() != actualVocEntry.getFrequencySize() ||
                expectedVocEntry.getNumBlocks() != actualVocEntry.getNumBlocks() ||
                expectedVocEntry.getBlockOffset() != actualVocEntry.getBlockOffset()
            ){
                System.out.println("\nERROR: TEST 3 FAILED\n");
                System.out.println("\texpected: "+expectedVocEntry);
                if(actualVocEntry==null)
                    System.out.println("\tfounded: null");
                else
                    System.out.println("\tfounded: "+actualVocEntry);
                return false;
            }
        }
*/
        System.out.println("\nTEST 3 ENDED SUCCESSFULLY\n");
        return true;
    }

    public static void main(String[] args) {
        verboseMode = false;
/*
        // initialize files and directories needed
        initializeFiles();

        // test 1: merging of 1 index without compression
        if(!test1(false))
            return;

        // initialize files and directories needed
        initializeFiles();

        // test 2: merging of 2 indexes without compression
        if(!test2(false))
            return;

        // initialize files and directories needed
        initializeFiles();

        Flags.saveFlags(true, true);
        Flags.initializeFlags();

        // test 3: merging of 3 indexes with compression
        if(!test2(true))
            return;
        */
        // initialize files and directories needed
        initializeFiles();

        Flags.saveFlags(true, true);
        Flags.initializeFlags();

        if(!test3(true)){
          return;
        }

    }


}
