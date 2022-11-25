package it.unipi.dii.aide.mircv.utils;

import it.unipi.dii.aide.mircv.beans.DocumentIndexEntry;
import it.unipi.dii.aide.mircv.beans.PostingList;
import it.unipi.dii.aide.mircv.beans.VocabularyEntry;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class StructureReaders {

    public static void readVocabulary(){
        //use DBMaker to create a DB object of HashMap stored on disk
        //provide location
        DB db = DBMaker.fileDB(ConfigurationParameters.getVocabularyPath()).make();

        //use the DB object to open the "myMap" HashMap
        Map<String, VocabularyEntry> vocabulary = (Map<String, VocabularyEntry>) db.hashMap("vocabulary")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA).createOrOpen();

        System.out.println(vocabulary.size());
        //read from map
        for(String term: vocabulary.keySet()){
            System.out.println(vocabulary.get(term));
        }

        //close to protect from data corruption
        db.close();
    }

    public static void readDocIndex(){
        //use DBMaker to create a DB object of HashMap stored on disk
        //provide location
        DB db = DBMaker.fileDB(ConfigurationParameters.getDocumentIndexPath()).make();

        //use the DB object to open the "myMap" HashMap
        List<DocumentIndexEntry> docIndex = (List<DocumentIndexEntry>) db.indexTreeList("docIndex", Serializer.JAVA).createOrOpen();

        System.out.println(docIndex.size());
        //read from map
        Iterator<DocumentIndexEntry> keys = docIndex.stream().iterator();
        while (keys.hasNext()) {
            System.out.println(keys.next());
        }

        //close to protect from data corruption
        db.close();
    }

    public static void readInvertedIndex() {
        DB db = DBMaker.fileDB(ConfigurationParameters.getVocabularyPath()).make();

        //use the DB object to open the "myMap" HashMap
        Map<String, VocabularyEntry> vocabularyDB = (Map<String, VocabularyEntry>) db.hashMap("vocabulary")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        Iterator<String> vocabulary = vocabularyDB.keySet().iterator();
        try (FileChannel fChan = (FileChannel) Files.newByteChannel(Paths.get(ConfigurationParameters.getInvertedIndexPath()), StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.CREATE)) {


            while (vocabulary.hasNext()) {

                VocabularyEntry term = vocabularyDB.get(vocabulary.next());
                long memOffset = term.getMemoryOffset();
                long freqOffset = term.getFrequencyOffset();
                long memSize = term.getMemorySize();
                long docSize = freqOffset - memOffset;

                // instantiation of MappedByteBuffer for integer list of docids
                MappedByteBuffer docBuffer = fChan.map(FileChannel.MapMode.READ_WRITE, memOffset, docSize);

                // instantiation of MappedByteBuffer for integer list of frequencies
                MappedByteBuffer freqBuffer = fChan.map(FileChannel.MapMode.READ_WRITE, freqOffset, memSize - docSize);

                // create the posting list for the term
                PostingList postingList = new PostingList(term.getTerm());
                for (int i = 0; i < term.getDf(); i++) {

                    Map.Entry<Integer, Integer> posting = new AbstractMap.SimpleEntry<>(docBuffer.getInt(), freqBuffer.getInt());
                    postingList.getPostings().add(posting);
                }

                System.out.println(postingList);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("--- DOCUMENT INDEX ---");
        readDocIndex();
        Thread.sleep(2000);

        System.out.println("--- VOCABULARY ---");
        readVocabulary();
        Thread.sleep(2000);

        System.out.println("--- INVERTED INDEX ---");
        readInvertedIndex();
    }
}
