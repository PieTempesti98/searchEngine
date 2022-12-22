package it.unipi.dii.aide.mircv.common.config;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class Flags {

    private static final String FLAGS_FILE_PATH = ConfigurationParameters.getFlagsFilePath();

    private static boolean compression;
    private static boolean stemStopRemoval;

    public static boolean initializeFlags(){

        try(
                FileInputStream flagsInStream = new FileInputStream(FLAGS_FILE_PATH);
                DataInputStream flagsDataStream = new DataInputStream(flagsInStream)
        ){

            compression = flagsDataStream.readBoolean();

            stemStopRemoval = flagsDataStream.readBoolean();

            return true;

        }catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public static boolean saveFlags(boolean compression,boolean stemStopRemoval){

        try(
                FileOutputStream flagsOutStream = new FileOutputStream(FLAGS_FILE_PATH);
                DataOutputStream flagsDataStream = new DataOutputStream(flagsOutStream)
        ){

            flagsDataStream.writeBoolean(compression);
            flagsDataStream.writeBoolean(stemStopRemoval);
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
}
