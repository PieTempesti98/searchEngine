package it.unipi.dii.aide.mircv.common.config;

import java.io.*;

/**
 * Utility class that stores the information about the collection statistics
 */
public class CollectionSize{
    /**
     * Size of the collection (and of the document index)
     */
    private static long collectionSize;

    /**
     * Size of the vocabulary
     */
    private static long vocabularySize;

    /**
     * Sum of the length of all documents
     */
    private static long totalDocLen;

    /**
     * Path to the collection size
     */
    private static String COLLECTION_STATISTICS_PATH = ConfigurationParameters.getCollectionStatisticsPath();

    static{
        if(!readFile()){
            collectionSize = 0;
            vocabularySize = 0;
            totalDocLen = 0;
        }
    }

    /**
     * read the collectionStatistics file
     * @return true if the read is successful
     */
    private static boolean readFile(){

        if(COLLECTION_STATISTICS_PATH==null)
            return false;
        File file = new File(COLLECTION_STATISTICS_PATH);

        if(!file.exists()){
            return false;
        }
        
        try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))){

            collectionSize = ois.readLong();
            vocabularySize = ois.readLong();
            totalDocLen = ois.readLong();

            return true;
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * @return the size of the collection
     */
    public static long getCollectionSize() {
        return collectionSize;
    }

    /**
     * @return the size of the vocabulary
     */
    public static long getVocabularySize() {
        return vocabularySize;
    }

    /**
     * @return the sum of the length of all documents
     */
    public static long getTotalDocLen() {return totalDocLen;}

    /**
     * write the class into the file
     * @return true if successful
     */
    private static boolean writeFile(){
        File file = new File(COLLECTION_STATISTICS_PATH);
        if(file.exists())
            if(!file.delete())
                return false;
        try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))){
            oos.writeLong(collectionSize);
            oos.writeLong(vocabularySize);
            oos.writeLong(totalDocLen);
            return true;

        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * update the collection size and save the value on disk
     * @param size the new size
     * @return true if write is successful
     */
    public static boolean updateCollectionSize(long size){
        collectionSize = size;
        return writeFile();
    }

    public static boolean updateDocumentsLenght(long len){
        totalDocLen = len;
        return writeFile();
    }

    /**
     * update the vocabulary size and save the value on disk
     *
     * @param size the new size
     */
    public static void updateVocabularySize(long size) {
        vocabularySize = size;
        writeFile();
    }

    public static void setTotalDocLen(long totalDocLen) {
        CollectionSize.totalDocLen = totalDocLen;
    }
    /** needed for testing purposes
     * @param collectionStatisticsPath: path to be set
     */
    public static void setCollectionStatisticsPath(String collectionStatisticsPath) {
        COLLECTION_STATISTICS_PATH = collectionStatisticsPath;

    }

    /** needed for testing purposes
     * @param size: path to be set
     */
    public static void setCollectionSize(int size) { CollectionSize.collectionSize = size;}
}
