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
    private final String VOCABULARY_PATH = "../data/partial_vocabulary/vocabulary";


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

    public boolean write_to_disk() {

        //open file channel
        try (FileChannel fChan = (FileChannel) Files.newByteChannel(
                Paths.get(VOCABULARY_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE)) {

            //initial position
            long position = 0;

            for (VocabularyEntry entry : this.values()) {

                // instantiation of MappedByteBuffer
                MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_WRITE, position, entry.getENTRY_SIZE());

                // Buffer not created
                if (buffer == null)
                    return false;

                //allocate char buffer to write term
                CharBuffer charBuffer = CharBuffer.allocate(entry.getTERM_SIZE());
                String term = entry.getTerm();

                //populate char buffer char by char
                for (int i = 0; i < term.length(); i++)
                    charBuffer.put(i, term.charAt(i));
                // Write the term into file
                buffer.put(StandardCharsets.UTF_8.encode(charBuffer));

                // Write the document frequency into file
                buffer.putInt(entry.getDf());

                // Write the term frequency into file
                buffer.putInt(entry.getTf());

                //wirte IDF into file
                buffer.putDouble(entry.getIdf());

                //write memory offset into file
                buffer.putLong(entry.getMemoryOffset());

                //write frequency offset into file
                buffer.putLong(entry.getFrequencyOffset());

                //write memory offset into file
                buffer.putLong(entry.getMemorySize());


                // update position for which we have to start writing on file
                position += entry.getENTRY_SIZE();

            }

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }



}
