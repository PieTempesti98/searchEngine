package it.unipi.dii.aide.mircv.common.config;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * utility class used to read and store the flags of the application
 */
public class Flags {

    /**
     * path of the flags file
     */
    private static final String FLAGS_FILE_PATH = ConfigurationParameters.getFlagsFilePath();

    /**
     * flag for enabling the compression
     */
    private static boolean compression;
    /**
     * flag for enabling the stopwords removal and the stemming
     */
    private static boolean stemStopRemoval;
    /**
     * flag for enabling the max score algorithm to score queries
     */
    private static boolean maxScore;

    /**
     * reads the flags from file and initialize the relative booleans
     *
     * @return true if successful
     */
    public static boolean initializeFlags() {

        try (
                FileInputStream flagsInStream = new FileInputStream(FLAGS_FILE_PATH);
                DataInputStream flagsDataStream = new DataInputStream(flagsInStream)
        ) {

            //read flags
            compression = flagsDataStream.readBoolean();
            stemStopRemoval = flagsDataStream.readBoolean();
            maxScore = flagsDataStream.readBoolean();

            return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    /**
     * updates the flag status and save them to file
     *
     * @param compressionFlag     to set compression
     * @param stemStopRemovalFlag to set setemming and stopword removal
     * @param maxScoreFlag        to set max score algorithm
     * @return true if successful
     */
    public static boolean saveFlags(boolean compressionFlag, boolean stemStopRemovalFlag, boolean maxScoreFlag) {

        try (
                FileOutputStream flagsOutStream = new FileOutputStream(FLAGS_FILE_PATH);
                DataOutputStream flagsDataStream = new DataOutputStream(flagsOutStream)
        ) {
            //update flags
            compression = compressionFlag;
            stemStopRemoval = stemStopRemovalFlag;
            maxScore = maxScoreFlag;

            //write flags to disk
            flagsDataStream.writeBoolean(compression);
            flagsDataStream.writeBoolean(stemStopRemoval);
            flagsDataStream.writeBoolean(maxScore);
            return true;

        }catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public static boolean isCompressionEnabled() {
        return compression;
    }

    public static boolean isStemStopRemovalEnabled() {
        return stemStopRemoval;
    }

    public static boolean isMaxScoreEnabled() {return maxScore;}


    public static void setCompression(boolean compression) {
        Flags.compression = compression;
    }

    public static void setStemStopRemoval(boolean stemStopRemoval) {
        Flags.stemStopRemoval = stemStopRemoval;
    }

    public static void setMaxScore(boolean maxScore) {
        Flags.maxScore = maxScore;
    }

}
