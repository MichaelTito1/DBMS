import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

// this class is a pointer to a row, used in the buckets of the grid index.
public class Reference implements Serializable, Comparable {
    private static final long serialVersionUID = 1L;
    private String pageAddress;
    private Object primaryKey;
    private Vector<Object> relevantValues;

    public Reference(String pageAddress, Object primaryKey, Vector<Object> relevantValues) {
        this.pageAddress = pageAddress;
        this.primaryKey = primaryKey;
        this.relevantValues = relevantValues;
    }

    public String getPageAddress() {
        return pageAddress;
    }

    public Object getPrimaryKey() {
        return primaryKey;
    }

    public Vector<Object> getRelevantValues() {
        return relevantValues;
    }

    /**
     * Compares between two references according to their primary key value.
     * @param o
     * @return
     */
    @Override
    public int compareTo(Object o) {
        Reference ref = (Reference) o;
        try {
            return compareObjects(this.primaryKey, ref.primaryKey);
        } catch (DBAppException e) {
            e.printStackTrace();
        }
        return Integer.MAX_VALUE;
    }

    // compares two objects according to their datatypes
    private int compareObjects(Object obj1, Object obj2) throws DBAppException{

        if( (obj1 instanceof java.lang.Integer) && (obj2 instanceof java.lang.Integer) ) {
            return ((Integer)obj1).compareTo((Integer) obj2);
        }
        else if( (obj1 instanceof java.lang.Double) && (obj2 instanceof java.lang.Double)){
            return ((Double)obj1).compareTo((Double) obj2);
        }
        else if( (obj1 instanceof java.lang.String) && (obj2 instanceof java.lang.String)){
            return ((String)obj1).compareTo((String) obj2);
        }
        else if( (obj1 instanceof java.util.Date) && (obj2 instanceof java.util.Date)){
            try {
                String newDate1 = convertDateFormat(obj1.toString());
                String newDate2 = convertDateFormat(obj2.toString());
                Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse(newDate1);
                Date d2 = new SimpleDateFormat("yyyy-MM-dd").parse(newDate2);
                return d1.compareTo(d2);
            } catch (ParseException e) {
                throw new DBAppException(e.getMessage());
            }
        }
        else
            throw new DBAppException("These objects can't be compared as their types don't match.");

    }

    // this method converts date of format like Sun Oct 17 00:00:00 EET 1948
    // to yyyy-MM-dd
    private String convertDateFormat(String searchVal) throws DBAppException {
        String oldFormat = "EEE MMM dd HH:mm:ss zzz yyyy";
        String newFormat = "yyyy-MM-dd";
        String newDate;
        SimpleDateFormat sdf = new SimpleDateFormat(oldFormat);
        Date d1 = null;
        // checking first if the entered string is of the targeted format

        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat(newFormat);
            Date d2 = sdf1.parse(searchVal);
            return searchVal;
        } catch (ParseException e1) {
            try {
                d1 = sdf.parse(searchVal);
            } catch (ParseException e) {
                throw new DBAppException(e.getMessage());
            }
        }

        sdf.applyPattern(newFormat);
        newDate = sdf.format(d1);
        return newDate;
    }
}
