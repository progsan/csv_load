package csv_load;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class CsvLoad {

	private static String JDBC_CONNECTION_URL = 
			"jdbc:postgresql://${host}:5432/${dbname}";
	
	private static final String HOST_REGEX = "\\$\\{host\\}";
	private static final String DBNAME_REGEX = "\\$\\{dbname\\}";
	
	private static String host = "localhost";
	private static String dataBaseName = "expert_sys";
	private static String fileName = "C:\\java\\rayon.csv";
	private static String userName = "postgres";
	private static String password = "postgres";

	
	public static void main(String[] args) {
		
		String tableName;
		
		//if (args.length == 0) {
		//	System.out.println("Необходимо передать на вход csv файл");
		//	return;
		//}
		
		// Формирование имени таблицы по имени файла
		tableName = fileName;
		
		int i = tableName.lastIndexOf('\\');
		if (i != -1) {
			tableName = tableName.substring(i+1);
		}
		
		i = tableName.lastIndexOf('.');
		if (i != -1) {
			tableName = tableName.substring(0, i);
		}
		
		// TODO: файл забрать из командной строки
		
		// TODO: пареметры создания таблицы и удаления существующих данных 
		// забрать из командной строки
		
		// TODO: настройки подключения 
				
		try {
			CsvLoader loader = new CsvLoader(getCon());
			loader.setSeparator(';');		
			
			loader.loadCSV(fileName, tableName, false, true);		
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Connection getCon() {
		Connection connection = null;
		
		String strCon = JDBC_CONNECTION_URL.replaceFirst(HOST_REGEX, host);
		strCon = strCon.replaceFirst(DBNAME_REGEX, dataBaseName);
		
		System.out.println("strCon: " + strCon);
		
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection(strCon, userName, password);

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return connection;
	}

}
