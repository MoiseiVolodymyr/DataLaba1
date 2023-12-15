package org.example;

public class DataLaba1
{
    public static void main( String[] args ) {
        CommitAndMessageResolver processor = new CommitAndMessageResolver();
        processor.process("10K.github.jsonl");
    }
}
