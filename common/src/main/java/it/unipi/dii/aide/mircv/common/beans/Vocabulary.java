package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class Vocabulary extends HashMap<String, VocabularyEntry>{

    private static Vocabulary instance = null;
    private final String VOCABULARY_PATH = ConfigurationParameters.getVocabularyPath();


    private Vocabulary(){
        //use DBMaker to create a DB object of HashMap stored on disk
        //provide location
        DB db = DBMaker.fileDB(ConfigurationParameters.getVocabularyPath()).fileMmapEnable().fileChannelEnable().make();

        //use the DB object to open the "myMap" HashMap
        Map<String, VocabularyEntry> vocabulary = (Map<String, VocabularyEntry>) db.hashMap("vocabulary")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA).createOrOpen();

        //read from map
        for(String term: vocabulary.keySet()){
            this.put(term, vocabulary.get(term));
        }

        //close to protect from data corruption
        db.close();
    }

    public static Vocabulary getInstance(){
        if(instance == null){
            instance = new Vocabulary();
        }
        return instance;
    }

    public double getIdf(String term){
        return this.get(term).getIdf();
    }

    public boolean writeToDisk() {


        long position = 0;

        for (VocabularyEntry entry : this.values()) {

            position = entry.writeEntryToDisk(0,this.VOCABULARY_PATH);
            if(position == -1)
                return false;

        }

        return true;


    }

    public boolean readFromDisk(){

        long position = 0;

        //read whole vocabulary from
        while(position >= 0){
            VocabularyEntry entry = new VocabularyEntry();

            //read entry and update position
            position = entry.readFromDisk(position,this.VOCABULARY_PATH);

            //populate vocabulary
            this.put(entry.getTerm(),entry);
        }

        //if position == -1 an error occurred during reading
        return position != -1;

    }

    /*
    *
    * @param term: term of which we want vocabulary entry
    * @returns: the vocabulary entry of given term
    **/
    public VocabularyEntry findEntry(String term){

        VocabularyEntry entry = new VocabularyEntry();

        //performs binary search for input term
        int start = 0;
        int end = 1000;
        int mid;
        String key;

        long entrySize = entry.getENTRY_SIZE();

        try (FileChannel fChan = (FileChannel) Files.newByteChannel(
                Paths.get(VOCABULARY_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE)) {

            while (start < end) {

                mid = (start + end) / 2;

                MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_ONLY, mid * entrySize, entrySize);

                if(buffer == null)
                    return null;

                // Read from file into the charBuffer, then pass to the string
                CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

                if(charBuffer.length() == 0)
                    return null;

                String[] encodedTerm = charBuffer.toString().split("\0");
                if(encodedTerm.length == 0)
                    return null;

                key = encodedTerm[0];

                if(term.equals(key)){
                    entry.readFromDisk(mid * entrySize,this.VOCABULARY_PATH);
                    break;
                }


                if(term.compareTo(key) > 0){

                    start = mid + 1;
                    continue;
                }

                end = mid - 1;


            }
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }


        return entry;
    }



}
