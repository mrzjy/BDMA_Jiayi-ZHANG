package similarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

class LongPair implements WritableComparable<LongPair> {

    private LongWritable key1;
    private LongWritable key2;
    
    public LongPair() {
        this.set(new LongWritable(0), new LongWritable(0));
    }
    
    public LongPair(LongWritable key1, LongWritable key2) {
        this.set(key1, key2);
    }
    
    public LongPair(Long key1, Long key2) {
        this.set(new LongWritable(key1), new LongWritable(key2));
    }

    public LongPair(String key1, String key2) {
        this.set(new LongWritable( new Long(key1)), new LongWritable( new Long(key2)));
    }

    public LongWritable getFirst() {
        return key1;
    }

    public LongWritable getSecond() {
        return key2;
    }

    public void set(LongWritable key1, LongWritable key2) {
        this.key1 = key1;
        this.key2 = key2;
    }    
    
    public void setFirst(LongWritable key1){
        this.key1 = key1;
    }
    
    public void setFirst(Long key1){
        this.key1 = new LongWritable(key1);
    }
    
    public void setSecond(LongWritable key2){
        this.key2 = key2;
    }
    
    public void setSecond(Long key2){
        this.key2 = new LongWritable(key2);
    }
    
    public long getSum(){
    	return this.key1.get()+this.key2.get();
    }
    
    public long getDiff(){
    	return Math.abs(this.key1.get()-this.key2.get());
    }
    
    public LongPair inverse(){
    	return new LongPair(key2, key1);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LongPair) {
            LongPair p1 = (LongPair) o;
            boolean b1 = key1.equals(p1.key1) && key2.equals(p1.key2);
            LongPair p2 = p1.inverse();
            boolean b2 = key1.equals(p2.key1) && key2.equals(p2.key2);
            return b1 || b2;
        }
        return false;
    }
    
    @Override
    public int compareTo(LongPair other) {
    	long cmp = this.getSum()-other.getSum();
    	long cmp_alter = this.getDiff() - other.getDiff();
    	if(cmp<0){
    		return 1;
    	}else if(cmp>0){
    		return -1;
    	}else if(cmp_alter<0){
    		return 1;
    	}else if(cmp_alter>0){
    		return -1;
    	}
    	return 0;
    }
    

    @Override
    public void readFields(DataInput input) throws IOException {
    	key1.readFields(input);
    	key2.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
    	key1.write(output);
    	key2.write(output);
    }

    @Override
    public String toString() {
        return key1.toString() + "," + key1.toString();
    }

}