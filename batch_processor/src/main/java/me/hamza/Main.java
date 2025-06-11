package me.hamza;

public class Main {

    public static void main(String[] args) {
        BatchProcessor processor = new BatchProcessor();
        try {
            processor.processVoiceCalls();
            processor.processSmsMessages();
            processor.processDataUsage();
        } finally {
            processor.close();
        }
    }
}