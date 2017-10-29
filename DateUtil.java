package csv_load;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DateUtil {

    private static ArrayList<SimpleDateFormat> 
            dateFormats = new ArrayList<SimpleDateFormat>() {
		private static final long serialVersionUID = 1L; 
		{
            add(new SimpleDateFormat("M/dd/yyyy"));
            add(new SimpleDateFormat("dd.M.yyyy"));
            add(new SimpleDateFormat("M/dd/yyyy hh:mm:ss a"));
            add(new SimpleDateFormat("dd.M.yyyy hh:mm:ss a"));
            add(new SimpleDateFormat("dd.MMM.yyyy"));
            add(new SimpleDateFormat("dd-MMM-yyyy"));
        }
    };
 

    public static Date convertToDate(String input) {
        Date date = null;
        if(null == input) {
            return null;
        }
        for (SimpleDateFormat format : dateFormats) {
            try {
            	format.setLenient(false);
                date = format.parse(input);
            } catch (ParseException e) {
            	// Ёто не дата.  акой-то другой формат данных
            }
            if (date != null) {
                break;
            }
        }
 
        return date;
    }

}
