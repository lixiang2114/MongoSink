import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import com.github.lixiang2114.flume.plugin.mdb.filter.MdbSinkFilter;

@SuppressWarnings("unchecked")
public class MdbLoggerFilter implements MdbSinkFilter{
	
	private String[] fields;
	
	private static String userName;
	
	private static String passWord;
	
	private String collectionName;
	
	private String dataBaseName;
	
	private String fieldSeparator;
	
	private static Pattern commaRegex;

	public String getDocId() {
		return this.fields[0];
	}

	public String getPassword() {
		return passWord;
	}

	public String getUsername() {
		return userName;
	}

	public String getCollectionName() {
		return this.collectionName;
	}

	public String getDataBaseName() {
		return this.dataBaseName;
	}

	public HashMap<String, Object>[] doFilter(String record) {
		String[] fieldValues = commaRegex.split(record);
	    HashMap<String, Object> map = new HashMap<String, Object>();
	    map.put(this.fields[0], fieldValues[0].trim());
	    map.put(this.fields[1], fieldValues[1].trim());
	    map.put(this.fields[2], fieldValues[2].trim());
	    return new HashMap[]{map};
	}

	public void filterConfig(Properties properties) {
		commaRegex = Pattern.compile(this.fieldSeparator);
	}
}
