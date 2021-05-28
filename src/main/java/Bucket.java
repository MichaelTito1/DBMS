import java.io.*;
import java.util.Vector;

public class Bucket implements Serializable {
    private static final long serialVersionUID = 1L;
    private int maxRows;
    private Vector<String> rowAddress;
    private Object min, max;
    private String path = "src\\main\\resources\\data";

    public Bucket(String path, int maxRows) {
        this.maxRows = maxRows;
        rowAddress = new Vector<String>();
        this.path = path;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public Vector<String> getRowAddress() {
        return rowAddress;
    }

    public Object getMin() {
        return min;
    }

    public void setMin(Object min) {
        this.min = min;
    }

    public void setMax(Object max) {
        this.max = max;
    }

    public Object getMax() {
        return max;
    }

    public void save() throws DBAppException {
        File f = new File(path);

        if(f.exists())
            f.delete();

        // Serializing the table
        ObjectOutputStream outStr;
        try {
            outStr = new ObjectOutputStream(new FileOutputStream(f));
            outStr.writeObject(this);
            outStr.close();
        } catch (FileNotFoundException e) {
            throw new DBAppException(e.getMessage());
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
    }
    //todo: 1. Create bucket
    //todo: 2. save bucket
    //todo: 3. bucket + page ?!!
}
