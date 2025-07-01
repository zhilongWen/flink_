package com.at.iceberg.demo;

import java.util.Objects;

public class WordCount {

    private String word;
    private int cnt;

    public WordCount(String word, int count) {
        this.word = word;
        this.cnt = count;
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

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", cnt=" + cnt +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WordCount wordCount = (WordCount) o;
        return cnt == wordCount.cnt && Objects.equals(word, wordCount.word);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word, cnt);
    }
}
