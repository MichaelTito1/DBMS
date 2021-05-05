import java.io.Serializable;

public class Row implements Serializable{
	private static final long serialVersionUID = 1L;
	private Object [] val;
	
	public Row(int size){
		val = new Object [size];
	}
	
	public void addValue(int index,Object value){
		val[index]=value;
	}
	
	public Object getValue(int idx){
		return val[idx];
	}
}
