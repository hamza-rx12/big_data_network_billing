package me.hamza;

public class Main {

    public static void main(String[] args) {
        BatchProcessor processor1 = new BatchProcessor();
        RatingProcessor processor2 = new RatingProcessor();
        try {
            processor1.processVoiceCalls();
            processor1.processSmsMessages();
            processor1.processDataUsage();
            processor2.processRatings();
        } finally {
            processor1.close();
            processor2.close();
        }
    }
}