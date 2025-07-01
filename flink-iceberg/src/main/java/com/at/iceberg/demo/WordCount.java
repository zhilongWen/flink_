package com.at.iceberg.demo;

public class WordCount {

    private String word;
    private int cnt;

    private String word_type;

    public WordCount(String word, int count) {
        this.word = word;
        this.cnt = count;
    }

    public WordCount(String word, String word_type) {
        this.word = word;
        this.word_type = word_type;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }

    public void setWord_type(String word_type) {
        this.word_type = word_type;
    }

    public String getWord_type() {
        return word_type;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", cnt=" + cnt +
                ", word_type='" + word_type + '\'' +
                '}';
    }
}
