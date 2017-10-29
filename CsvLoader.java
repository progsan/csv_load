package csv_load;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVReader;

public class CsvLoader {
	private static final 
		String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
	
	private static final
		String SQL_CREATE = "CREATE TABLE ${table}(";
		
	private static final String TABLE_REGEX = "\\$\\{table\\}";
	private static final String KEYS_REGEX = "\\$\\{keys\\}";
	private static final String VALUES_REGEX = "\\$\\{values\\}";
	
	private Connection connection;
	private char separator;
	

	public CsvLoader(Connection connection) {
		this.connection = connection;

		setSeparator(';');
	}
	
	public void loadCSV(
			String csvFile, 
			String tableName,
			boolean createBeforeLoad,
			boolean truncateBeforeLoad) throws Exception 
	{

		//CSVReader csvReader = null;
		if(null == this.connection) {
			throw new Exception("Not a valid connection.");
		}
		
		final CSVParser parser =
				new CSVParserBuilder()
				.withSeparator(this.separator)
				.withIgnoreQuotations(true)
				.build();
		
					
		final CSVReader csvReader =
				new CSVReaderBuilder(new FileReader(csvFile))
				//.withSkipLines(1)
				.withCSVParser(parser)
				.build();			
	
		//} catch (Exception e) {
		//	e.printStackTrace();
		//	throw new Exception("Error occured while executing file. "
		//			+ e.getMessage());
		//}
	
		String[] headerRow = csvReader.readNext();
	
		if (null == headerRow) {
			throw new FileNotFoundException(
					"В данном CSV не заданы столбцы." +
					"Please check the CSV file format.");
		}
		
		// TODO: если поле id отсутсвует, и стоит флаг автоматической генерации 
		// этого поля - формируем его
	
		String questionmarks = StringUtils.repeat("?,", headerRow.length);
		questionmarks = (String) questionmarks.subSequence(0, questionmarks
				.length() - 1);
	
		String query = SQL_INSERT.replaceFirst(TABLE_REGEX, tableName);
		query = query
				.replaceFirst(KEYS_REGEX, StringUtils.join(headerRow, ","));
		query = query.replaceFirst(VALUES_REGEX, questionmarks);
	
		System.out.println("Query: " + query);
		
		String createQuery = SQL_CREATE.replaceFirst(TABLE_REGEX, tableName);
		for (String col : headerRow) {
			createQuery += col + " ";
			if (col.equalsIgnoreCase("id")) {
				createQuery += "integer";
			}
			else if (col.equalsIgnoreCase("name")){
				createQuery += "text";
			}
			else {
				createQuery += "real";
			}
			createQuery += ", ";
		}
		createQuery = (String) createQuery.subSequence(0, createQuery.length() - 2);
		createQuery += ")";
		
		System.out.println("createQuery: " + createQuery);
		
		Connection con = null;
		String[] nextLine;		
		PreparedStatement ps = null;
		try {
			con = this.connection;
			con.setAutoCommit(false);
			ps = con.prepareStatement(query);		
			
			// Проверить таблицу на существование
			
			
			// Создание таблицы перед заполнением
			if (createBeforeLoad) {
				con.createStatement().execute(createQuery);
			}
			
			// Затереть существующую информацию таблицы
			if(truncateBeforeLoad) {				
				con.createStatement().execute("DELETE FROM " + tableName);
			}
	
			final int batchSize = 1000;
			int count = 0;
			Date date = null;
			while ((nextLine = csvReader.readNext()) != null) {
	
				if (null != nextLine) {
					int index = 1;
					for (String string : nextLine) {
						date = DateUtil.convertToDate(string);
						if (null != date) {
							ps.setDate(index, new java.sql.Date(date.getTime()));
						}
						else {
							if (index == 1) {
								ps.setInt(index, Integer.parseInt(string));									
							}
							else {
								ps.setString(index, string);
							}							
						}
						index++;
					}
					ps.addBatch();
				}
				if (++count % batchSize == 0) {
					ps.executeBatch();
				}
			}
			ps.executeBatch(); 
			con.commit();
		} catch (Exception e) {
			con.rollback();
			e.printStackTrace();
			throw new Exception(
					"Error occured while loading data from file to database."
							+ e.getMessage());
		} finally {
			if (null != ps)
				ps.close();
			if (null != con)
				con.close();
	
			csvReader.close();
		}
	}

	public char getSeparator() {
		return separator;
	}
	
	public void setSeparator(char seprator) {
		this.separator = seprator;
	}

}
