package dio.datamodel;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by IgorPc on 6/4/2019.
 */
public class PairCouple implements Serializable {
    private String[] words;
    private Set<String> setForEquality;
    private int count;
    private int level;

    public PairCouple(String... words) {
        increment();
        this.setForEquality=new LinkedHashSet<>(Arrays.asList(words));
        this.words=words;
        level=0;
    }

    public static class CountComparator implements Comparator<PairCouple>{

        @Override
        public int compare(PairCouple o1, PairCouple o2) {
            return o1.getCount()-o2.getCount();
        }
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public String[] getWords() {
        return words;
    }

    public void setWords(String[] words) {
        this.words = words;
    }

    public void increment() {
        this.count++;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairCouple that = (PairCouple) o;
        return words.length == that.words.length && setForEquality.equals(that.setForEquality);
    }

    @Override
    public int hashCode() {
        int hash=0;
        for (String word : words) {
            hash+=word.hashCode();
        }

        return 31*hash;
    }

    @Override
    public String toString() {
        return "PairCouple{" +
                "words=" + Arrays.toString(words) +
                ", count=" + count +
                '}';
    }
}
