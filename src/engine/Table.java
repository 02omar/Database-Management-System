package engine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Hashtable;

import exceptions.DBAppException;

public class Table implements java.io.Serializable {
	private static final long serialVersionUID = 3394126077582660496L;
	String strTableName;
	String strClusteringKeyColumn;
	Hashtable<String, String> htblColNameType;
	Hashtable<String, String> htblColNameMin;
	Hashtable<String, String> htblColNameMax;
	int numOfPages = 0;
	double numOfIndices = 0;

	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws IOException, DBAppException {
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;
		this.htblColNameMin = htblColNameMin;
		this.htblColNameMax = htblColNameMax;
		validate(htblColNameType, htblColNameMin, htblColNameMax);
		createMetaData();
		writeToFile(this);
	}
	
	public static void writeToFile(Table table) {
        try {
        	File file = new File("src/resources/docs/tables/" + table.strTableName + ".ser");
    		FileOutputStream out = new FileOutputStream("src/resources/docs/tables/" + table.strTableName + ".ser");
            ObjectOutputStream oos = new ObjectOutputStream(out);
			oos.writeObject(table);
			out.close();
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
	
	public static Table readFromFile(String strTableName)
	{
		if(DBApp.doesTableExist(strTableName))
		{
			try {
				FileInputStream fileIn = new FileInputStream("src/resources/docs/tables/" + strTableName + ".ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				Table table = (Table) in.readObject();
				in.close();
				fileIn.close();
				return table;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public void createMetaData() throws IOException {
		File dir = new File("src/resources/data/metadata.csv");
		FileWriter fileWriter = new FileWriter(dir, true);

		BufferedWriter bw = new BufferedWriter(fileWriter);
		PrintWriter out = new PrintWriter(bw);

		for (String colName : htblColNameType.keySet()) {
			out.print(strTableName + ", " + colName + ", " + htblColNameType.get(colName) + ", "
					+ (colName.equals(strClusteringKeyColumn) ? "True, " : "False, ") + "null" + ", " + "null"
					+ ", " + htblColNameMin.get(colName) + ", " + htblColNameMax.get(colName) + "\n");
		}

		out.println();
		out.flush();
		out.close();
		fileWriter.close();
	}
	
	public static void validate(Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException
	{
		String dataType;
		
		for(String colName: htblColNameType.keySet())
		{
			dataType = htblColNameType.get(colName);
			
			if(dataType.equals("java.lang.Integer") == false && dataType.equals("java.lang.Double") == false && dataType.equals("java.lang.String") == false && dataType.equals("java.util.Date") == false)
				throw new DBAppException("Invalid Type");
		}
	}
}
