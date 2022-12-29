package it.unipi.dii.aide.mircv.common.config;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class Flags {

    private static final String FLAGS_FILE_PATH = ConfigurationParameters.getFlagsFilePath();

    private static boolean compression;
    private static boolean stemStopRemoval;
    private static boolean maxScore;

    public static boolean initializeFlags(){

        try(
                FileInputStream flagsInStream = new FileInputStream(FLAGS_FILE_PATH);
                DataInputStream flagsDataStream = new DataInputStream(flagsInStream)
        ){

            //read flags
            compression = flagsDataStream.readBoolean();
            stemStopRemoval = flagsDataStream.readBoolean();
            maxScore = flagsDataStream.readBoolean();

            return true;

        }catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public static boolean saveFlags(boolean compressionFlag,boolean stemStopRemovalFlag,boolean maxScoreFlag){

        try(
                FileOutputStream flagsOutStream = new FileOutputStream(FLAGS_FILE_PATH);
                DataOutputStream flagsDataStream = new DataOutputStream(flagsOutStream)
        ){
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

    public static void setMaxScore(boolean enable){ maxScore = enable;}


    /**
     * used for testing purposes only
     */
    public static void setStemStopRemoval(boolean stemStopRemoval) {Flags.stemStopRemoval = stemStopRemoval;}
}
