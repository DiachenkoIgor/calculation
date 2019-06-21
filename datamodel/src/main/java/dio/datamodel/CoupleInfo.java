package dio.datamodel;

/**
 * Created by IgorPc on 6/10/2019.
 */
public class CoupleInfo {
    private int count;
    private int level;

    public CoupleInfo(Integer count, int level) {
        this.count = count;
        this.level = level;
    }

    public void incrementCount(){
        count++;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }
}
