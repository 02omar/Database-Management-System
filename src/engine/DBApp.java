package engine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

import exceptions.DBAppException;

public class DBApp {

	static int maxTuplesPerPage;
	
	public static void init() throws IOException {
		Properties properties = new Properties();
		FileInputStream fis = new FileInputStream("src/resources/config/DBApp.config");
		properties.load(fis);
		maxTuplesPerPage = Integer.parseInt(properties.getProperty("MaximumRowsCountinTablePage"));
		Node.maxEntriesInOctreeNode = Integer.parseInt(properties.getProperty("MaximumEntriesinOctreeNode"));
	}

	public static boolean doesTableExist(String strTableName) 
	{
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) 
		{
			String line;
			while ((line = br.readLine()) != null) 
			{
				String[] values = line.split(",");
				
				if (values[0].equals(strTableName))
					return true;
			}
		} 
		catch (Exception e) 
		{
			return false;
		}
		return false;
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {
		if (!doesTableExist(strTableName)) 
		{
			try 
			{
				new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
			} 
			catch (IOException e) 
			{
				System.out.println("IOException occurred");
			}
		} 
		else
			throw new DBAppException("Can not create a table that already exists");
	}
	
	public static void validateEntry(String strTableName, Hashtable<String, Object> htblColNameValue, boolean insert) throws FileNotFoundException, IOException, DBAppException
	{
		try(BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv")))
		{
			String line;
			
			while((line = br.readLine()) != null)
			{
				String[] values = line.split(",");
				if(values[0].equals(strTableName))
				{
					Object givenValue = htblColNameValue.get(values[1].substring(1));
					if(givenValue == null)
						continue;
					
					if(givenValue == "null")
						continue;
					
					if(values[2].substring(1).equals("java.lang.String"))
					{
						if(!(givenValue instanceof String))
							throw new DBAppException("Invalid data type");
						
						if(((String) givenValue).compareToIgnoreCase(values[6].substring(1)) < 0)
							throw new DBAppException("Can not insert value less than min accepted value");
						
						if(((String) givenValue).compareToIgnoreCase(values[7].substring(1)) > 0)
							throw new DBAppException("Can not insert value greater than max accepted value");
					}
					
					if(values[2].substring(1).equals("java.lang.Integer"))
					{	
						if(!(givenValue instanceof Integer))
							throw new DBAppException("Invalid data type");
						
						if((int)givenValue < Integer.parseInt(values[6].substring(1)))
							throw new DBAppException("Can not insert value less than min accepted value");
						
						if((int)givenValue > Integer.parseInt(values[7].substring(1)))
							throw new DBAppException("Can not insert value greater than max accepted value");
					}
					
					if(values[2].substring(1).equals("java.lang.Double"))
					{
						if(!(givenValue instanceof Double))
							throw new DBAppException("Invalid data type");
						
						if((double)givenValue < Double.parseDouble(values[6].substring(1)))
							throw new DBAppException("Can not insert value less than min accepted value");
						
						if((double)givenValue > Double.parseDouble(values[7].substring(1)))
							throw new DBAppException("Can not insert value greater than max accepted value");
					}
					
					if(values[2].substring(1).equals("java.util.Date"))
					{
						try 
						{
							SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD");  
							Date inputDate = dateFormat.parse((String) givenValue);
							Date minDate = dateFormat.parse(values[6].substring(1));
							Date maxDate = dateFormat.parse(values[7].substring(1));
							
							if((inputDate).compareTo(minDate) < 0)
								throw new DBAppException("Can not insert value less than min accepted value");
							
							if((inputDate).compareTo(maxDate) > 0)
								throw new DBAppException("Can not insert value greater than max accepted value");
							
						} 
						catch (ParseException e) 
						{
							e.printStackTrace();
						}
					}
					
					if(givenValue != null && values[3].substring(1).equals("True") && insert == true)
					{
						validateDuplicates(strTableName, htblColNameValue.get(values[1].substring(1)));
					}
				}
			}
		}
	}
	
	public static void validateDuplicates(String strTableName, Object clusteringKey) throws FileNotFoundException, IOException, DBAppException
	{
		Table table = Table.readFromFile(strTableName);
		
		if(table.numOfPages == 0)
			return;
		
		Page page;
		
		for(int i = 1; i <= table.numOfPages; i++)
		{
			page = Page.readFromFile(strTableName, i);
			for(int j = 0; j < page.rows.size(); j++)
			{
				if(page.rows.get(j).get(getClusteringKeyName(strTableName)).equals(clusteringKey))
					throw new DBAppException("Primary key already exists. Can not have duplicate primary keys");
			}
		}
	}
	
	public static Object getClusteringKeyName(String strTableName) throws FileNotFoundException, IOException
	{
		try(BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv")))
		{
			String line;
			while((line = br.readLine()) != null)
			{
				String[] values = line.split(",");
				
				if(values[0].equals(strTableName))
				{
					if(values[3].substring(1).equals("True"))
						return values[1].substring(1);
				}
			}
		}
		
		return "";
	}
	
	public static String getClusteringKeyType(String strTableName) throws FileNotFoundException, IOException
	{
		try(BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv")))
		{
			String line;
			while((line = br.readLine()) != null)
			{
				String[] values = line.split(",");
				
				if(values[0].equals(strTableName))
				{
					if(values[3].substring(1).equals("True"))
						return values[2].substring(1);
				}
			}
		}
		
		return "";
	}
	
	public static boolean check(String strTableName, Hashtable<String, Object> htblColNameValue, Page page, boolean min, int check) throws FileNotFoundException, IOException
	{
		String clusteringKeyType = getClusteringKeyType(strTableName);
		
		if(clusteringKeyType.equals("java.lang.Integer"))
		{
			int newValue = (int) htblColNameValue.get(getClusteringKeyName(strTableName));
			
			if((newValue >= (Integer) page.minValue) && (newValue <= (Integer) page.maxValue) && check == 1 && min == false)
				return true;
			
			if((newValue < (Integer) page.minValue) && min && check == 0)
				return true;
			
			if(newValue > (Integer) page.maxValue && !min && check == 0)
				return true;
		}
		
		if(clusteringKeyType.equals("java.lang.Double"))
		{
			double newValue = (double) htblColNameValue.get(getClusteringKeyName(strTableName));
			
			if((newValue >= (Double) page.minValue) && (newValue <= (Double) page.maxValue) && check == 1 && min == false)
				return true;
			
			if(newValue < (Double) page.minValue && min && check == 0)
				return true;
			
			if(newValue > (Double) page.maxValue && !min && check == 0)
				return true;
		}
		
		if(clusteringKeyType.equals("java.lang.String"))
		{
			String newValue = (String) htblColNameValue.get(getClusteringKeyName(strTableName));
			
			if(newValue.compareToIgnoreCase((String) page.minValue) >= 0 && (newValue.compareToIgnoreCase((String) page.maxValue)) <= 0 && check == 1 && min == false)
				return true;
			
			if(newValue.compareToIgnoreCase((String) page.minValue) < 0 && min && check == 0)
				return true;
			
			if(newValue.compareToIgnoreCase((String) page.maxValue) > 0 && !min && check == 0)
				return true;
		}
		
		if(clusteringKeyType.equals("java.util.Date"))
		{
			Date newValue = (Date) htblColNameValue.get(getClusteringKeyName(strTableName));
			
			if(newValue.compareTo((Date) page.minValue) >= 0 && (newValue.compareTo((Date) page.maxValue)) <= 0 && check == 1 && min == false)
				return true;
			
			if(newValue.compareTo((Date) page.minValue) < 0 && min && check == 0)
				return true;
			
			if(newValue.compareTo((Date) page.maxValue) > 0 && !min && check == 0)
				return true;
		}
		
		return false;
	}
	
	public static int binarySearch(String strTableName, Page page, Object newValue) throws FileNotFoundException, IOException
	{
		String clusteringKeyType = getClusteringKeyType(strTableName);
		
		int left = 0;
		int right = page.rows.size() - 1;
		
		if(clusteringKeyType.equals("java.lang.Integer"))
		{
			while(left <= right)
			{
				int middle = (left + right) / 2;
				
				if((int) newValue > (int) page.rows.get(middle).get(getClusteringKeyName(strTableName)))
					left = middle + 1;
				else
					right = middle - 1;
			}
		}
		
		if(clusteringKeyType.equals("java.lang.Double"))
		{
			while(left <= right)
			{
				int middle = (left + right) / 2;
				
				if((double) newValue > (double) page.rows.get(middle).get(getClusteringKeyName(strTableName)))
					left = middle + 1;
				else
					right = middle - 1;
			}
		}
		
		if(clusteringKeyType.equals("java.lang.String"))
		{
			while(left <= right)
			{
				int middle = (left + right) / 2;
				
				if(((String) newValue).compareToIgnoreCase((String) page.rows.get(middle).get(getClusteringKeyName(strTableName))) > 0)
					left = middle + 1;
				else
					right = middle - 1;
			}
		}
		
		if(clusteringKeyType.equals("java.util.Date"))
		{
			while(left <= right)
			{
				int middle = (left + right) / 2;
				
				if(((Date) newValue).compareTo((Date) page.rows.get(middle).get(getClusteringKeyName(strTableName))) > 0)
					left = middle + 1;
				else
					right = middle - 1;
			}
		}
		
		return left;
	}
	
	public static int binarySearch2(String strTableName, Page page, Object value) throws FileNotFoundException, IOException
	{
		String clusteringKeyType = getClusteringKeyType(strTableName);
		
		int left = 0;
		int right = page.rows.size() - 1;
		
		if(clusteringKeyType.equals("java.lang.Integer"))
		{
			while(left <= right)
			{
				int middle = (left + right) / 2;
				
				if(page.rows.get(middle).get(getClusteringKeyName(strTableName)).equals(value))
					return middle;
				
				else
				{
					if((int) value > (int) page.rows.get(middle).get(getClusteringKeyName(strTableName)))
						left = middle + 1;
					else
						right = middle - 1;
				}
			}
		}
		
		if(clusteringKeyType.equals("java.lang.Double"))
		{
			while(left <= right)
			{
				int middle = (left + right) / 2;
				
				if(page.rows.get(middle).get(getClusteringKeyName(strTableName)).equals(value))
					return middle;
				
				else
				{
					if((double) value > (double) page.rows.get(middle).get(getClusteringKeyName(strTableName)))
						left = middle + 1;
					else
						right = middle - 1;
				}
			}
		}
		
		if(clusteringKeyType.equals("java.lang.String"))
		{
			while(left <= right)
			{
				int middle = (left + right) / 2;
				
				if(page.rows.get(middle).get(getClusteringKeyName(strTableName)).equals(value))
					return middle;
				
				else
				{
					if(((String) value).compareToIgnoreCase((String) page.rows.get(middle).get(getClusteringKeyName(strTableName))) > 0)
						left = middle + 1;
					else
						right = middle - 1;
				}
			}
		}
		
		if(clusteringKeyType.equals("java.util.Date"))
		{
			while(left <= right)
			{
				int middle = (left + right) / 2;
				
				if(page.rows.get(middle).get(getClusteringKeyName(strTableName)).equals(value))
					return middle;
				
				else
				{
					if(((Date) value).compareTo((Date) page.rows.get(middle).get(getClusteringKeyName(strTableName))) > 0)
						left = middle + 1;
					else
						right = middle - 1;
				}
			}
		}
		
		return -1;
	}
	
	public void createNewPage(Table table, Hashtable<String, Object> htblColNameValue) throws FileNotFoundException, IOException
	{
		Page page = new Page();
		table.numOfPages++;
		
		page.rows.add(htblColNameValue);
		page.minValue = htblColNameValue.get(getClusteringKeyName(table.strTableName));
		page.maxValue = htblColNameValue.get(getClusteringKeyName(table.strTableName));
		Page.writeToFile(page, table.strTableName, table.numOfPages);
		
		if(table.numOfIndices > 0)
		{
			Octree octree;
			HashSet<String> indices = getIndicesOnTable(table.strTableName);
			
			for(String indexName : indices)
			{
				octree = Octree.readFromFile(indexName);
				
				octree.delete(page.rows.get(0).get(getClusteringKeyName(table.strTableName)), page.rows.get(0));
				octree.insert(page.rows.get(0).get(getClusteringKeyName(table.strTableName)), table.numOfPages, page.rows.get(0));
				
				Octree.writeToFile(octree);
			}
			
//			insertIntoIndex(table.strTableName, page.rows.get(0), table.numOfPages);
		}

		Table.writeToFile(table);
	}
	
	public void shiftDown(Table table, int idx, Hashtable<String, Object> shiftedRow) throws FileNotFoundException, IOException
	{
		if(idx > table.numOfPages)
		{
			createNewPage(table, shiftedRow);
			return;
		}
		
		Page tmpPage = Page.readFromFile(table.strTableName, idx);
		if(idx == table.numOfPages && tmpPage.rows.size() == maxTuplesPerPage)
		{
			tmpPage.rows.add(0, shiftedRow);
			tmpPage.minValue = shiftedRow.get(getClusteringKeyName(table.strTableName));
			shiftedRow = tmpPage.rows.remove(tmpPage.rows.size() - 1);
			tmpPage.maxValue = tmpPage.rows.get((tmpPage.rows.size() - 1)).get(getClusteringKeyName(table.strTableName));
			
			Page.writeToFile(tmpPage, table.strTableName, idx);
			createNewPage(table, shiftedRow);
			return;
		}
		
		Page page;
		for(int i = idx; i <= table.numOfPages; i++)
		{
			page = Page.readFromFile(table.strTableName, i);
			page.rows.add(0, shiftedRow);
			page.minValue = shiftedRow.get(getClusteringKeyName(table.strTableName));
			Page.writeToFile(page, table.strTableName, i);
			
			Octree octree;
			HashSet<String> indices = getIndicesOnTable(table.strTableName);
			
			for(String indexName : indices)
			{
				octree = Octree.readFromFile(indexName);
				
				octree.delete(shiftedRow.get(getClusteringKeyName(table.strTableName)), shiftedRow);
				octree.insert(shiftedRow.get(getClusteringKeyName(table.strTableName)), i, shiftedRow);
				
				Octree.writeToFile(octree);
			}
			
			if(page.rows.size() <= maxTuplesPerPage)
				break;
			
			shiftedRow = page.rows.remove(page.rows.size() - 1);
			page.maxValue = page.rows.get((page.rows.size() - 1)).get(getClusteringKeyName(table.strTableName));
			Page.writeToFile(page, table.strTableName, i);
			
			indices = getIndicesOnTable(table.strTableName);
			
			for(String indexName : indices)
			{
				octree = Octree.readFromFile(indexName);
				
				octree.delete(shiftedRow.get(getClusteringKeyName(table.strTableName)), shiftedRow);
				octree.insert(shiftedRow.get(getClusteringKeyName(table.strTableName)), i, shiftedRow);
				
				Octree.writeToFile(octree);
			}
			
			if(i == table.numOfPages)
			{
				createNewPage(table, shiftedRow);
				break;
			}
		}
	}
	
	public static HashSet<String> getIndicesOnTable(String strTableName)
	{
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) 
		{
			String line;
			HashSet<String> indices = new HashSet<String>();
			while ((line = br.readLine()) != null) 
			{
				String[] values = line.split(",");
				
				if (values[0].equals(strTableName))
				{
					if(!values[4].substring(1).equals("null"))
						indices.add(strTableName + "_" + values[4].substring(1));
				}
			}
			
			return indices;
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			return null;
		}
	}
	
	public void insertIntoIndex(String strTableName, Hashtable<String, Object> htblColNameValue, int i)
	{
		HashSet<String> indices = getIndicesOnTable(strTableName);
		Octree octree;
		
		for(String indexName : indices)
		{
			octree = Octree.readFromFile(indexName);
			
			try {
				octree.insert(htblColNameValue.get(getClusteringKeyName(strTableName)), i, htblColNameValue);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			Octree.writeToFile(octree);
		}
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			validateEntry(strTableName, htblColNameValue, true);
			
			if(htblColNameValue.get(getClusteringKeyName(strTableName)) == null)
				throw new DBAppException("Can not insert a record with no primary key");
			
			Table table = Table.readFromFile(strTableName);
			
			if(htblColNameValue.size() < table.htblColNameType.size())
			{
				for(String colName : table.htblColNameType.keySet())
				{
					if(!(htblColNameValue.containsKey(colName)))
						htblColNameValue.put(colName, "null");
				}
			}
				
			for(String colName : htblColNameValue.keySet())
			{
				if(table.htblColNameType.get(colName) == null)
					throw new DBAppException("Can not insert column " + colName + " since it doesn't exist.");
			}
				
			if(table.numOfPages == 0)
			{
				createNewPage(table, htblColNameValue);
				return;
			}
				
			Page page;
			boolean lessThanMin;
			boolean greaterThanMax;
			boolean inBetween;
				
			int size = table.numOfPages;
				
			for(int i = 1; i <= size; i++)
			{
				page = Page.readFromFile(strTableName, i);
				lessThanMin = check(strTableName, htblColNameValue, page, true, 0);
				greaterThanMax = check(strTableName, htblColNameValue, page, false, 0);
				inBetween = check(strTableName, htblColNameValue, page, false, 1);
				
				if(inBetween)
				{
					int pos = binarySearch(strTableName, page, htblColNameValue.get(getClusteringKeyName(strTableName)));
					page.rows.add(pos, htblColNameValue);
					
					if(page.rows.size() > maxTuplesPerPage)
					{
						Hashtable<String, Object> shiftedRow = page.rows.remove(page.rows.size() - 1);
						page.maxValue = page.rows.get((page.rows.size() - 1)).get(getClusteringKeyName(strTableName));
						
						shiftDown(table, i + 1, shiftedRow);
					}
					Page.writeToFile(page, strTableName, i);
					
					if(table.numOfIndices > 0)
						insertIntoIndex(strTableName, page.rows.get(pos), i);
					
					break;
				}
					
				if(lessThanMin)
				{
					page.rows.add(0, htblColNameValue);
					page.minValue = htblColNameValue.get(getClusteringKeyName(table.strTableName));
						
					if(page.rows.size() > maxTuplesPerPage)
					{
						Hashtable<String, Object> shiftedRow = page.rows.remove(page.rows.size() - 1);
						page.maxValue = page.rows.get((page.rows.size() - 1)).get(getClusteringKeyName(strTableName));
						
						shiftDown(table, i + 1, shiftedRow);
					}
					Page.writeToFile(page, strTableName, i);
					
					if(table.numOfIndices > 0)
						insertIntoIndex(strTableName, page.rows.get(0), i);
					
					break;
				}
					
				if(greaterThanMax)
				{
					if(page.rows.size() >= maxTuplesPerPage)
					{
						if(i == table.numOfPages)
						{
							createNewPage(table, htblColNameValue);
							break;
						}
					}
					else
					{
						page.rows.add(page.rows.size(), htblColNameValue);
						page.maxValue = htblColNameValue.get(getClusteringKeyName(table.strTableName));
						Page.writeToFile(page, strTableName, i);
						
						if(table.numOfIndices > 0)
						{
							insertIntoIndex(strTableName, page.rows.get(page.rows.size() - 1), i);
						}
						
						break;
					}
				}
			}
			
			Table.writeToFile(table);
			
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException
	{
		try
		{
			validateEntry(strTableName, htblColNameValue, true);
			
			if(htblColNameValue.get(getClusteringKeyName(strTableName)) != null)
				throw new DBAppException("Can not update primary key");
			
			Table table = Table.readFromFile(strTableName);
			
			for(String colName : htblColNameValue.keySet())
			{
				if(table.htblColNameType.get(colName) == null)
					throw new DBAppException("Can not update column " + colName + " since it doesn't exist.");
			}
			
			Page page;
			
			Hashtable<String, Object> clusteringKey = new Hashtable<String, Object>();
			String clusteringKeyType = getClusteringKeyType(strTableName);
			
			if(clusteringKeyType.equals("java.lang.Integer"))
				clusteringKey.put((String) getClusteringKeyName(strTableName), Integer.parseInt(strClusteringKeyValue));
			
			if(clusteringKeyType.equals("java.lang.Double"))
				clusteringKey.put((String) getClusteringKeyName(strTableName), Double.parseDouble(strClusteringKeyValue));
			
			if(clusteringKeyType.equals("java.lang.String"))
				clusteringKey.put((String) getClusteringKeyName(strTableName), (String) strClusteringKeyValue);

			if(clusteringKeyType.equals("java.util.Date"))
			{
				SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD");  
				try {
					clusteringKey.put((String) getClusteringKeyName(strTableName), dateFormat.parse(strClusteringKeyValue));
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			
			boolean inBetween;
			boolean flag = false;
			int size = table.numOfPages;
			
			for(int i = 1; i <= size; i++)
			{
				page = Page.readFromFile(strTableName, i);
				inBetween = check(strTableName, clusteringKey, page, false, 1);
				flag = false;
				if(inBetween)
				{
					int pos = binarySearch2(strTableName, page, clusteringKey.get(getClusteringKeyName(strTableName)));
					if(pos == -1)
						throw new DBAppException("Can not update record that does not exist");
					
					Hashtable<String, Object> oldValue = page.rows.get(pos);
					Octree octree;
					HashSet<String> indices = getIndicesOnTable(strTableName);
					
					for(String indexName : indices)
					{
						octree = Octree.readFromFile(indexName);

						octree.update(page.rows.get(pos).get(getClusteringKeyName(strTableName)), oldValue, htblColNameValue);
						
						Octree.writeToFile(octree);
					}
					
					for(String colName : htblColNameValue.keySet())
						page.rows.get(pos).replace(colName, htblColNameValue.get(colName));
					
					Page.writeToFile(page, strTableName, i);
					flag = true;
					break;
				}
			}
			
			if(!flag)
				throw new DBAppException("Can not update record that does not exist");
			
			Table.writeToFile(table);
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public static boolean satisfies(Hashtable<String, Object> row, Hashtable<String, Object> htblColNameValue)
	{
		for(String colName : htblColNameValue.keySet())
		{
			if(!(htblColNameValue.get(colName).equals(row.get(colName))))
				return false;
		}
		
		return true;
	}
	
	public static void shiftUp(Table table) throws FileNotFoundException, IOException
	{
		Page page;
		Page tmpPage;
		int c;
		
		for(int i = 1; i <= table.numOfPages; i++)
		{
			page = Page.readFromFile(table.strTableName, i);
			c = i + 1;
			
			while(page.rows.size() < maxTuplesPerPage)
			{
				if(i >= table.numOfPages)
					break;
				
				if(c > table.numOfPages)
					break;
				
				tmpPage = Page.readFromFile(table.strTableName, c);
				
				for(int j = 1; j <= tmpPage.rows.size(); j++)
				{
					Octree octree;
					HashSet<String> indices = getIndicesOnTable(table.strTableName);
					
					for(String indexName : indices)
					{
						octree = Octree.readFromFile(indexName);
						
						octree.delete(tmpPage.rows.get(0).get(getClusteringKeyName(table.strTableName)), tmpPage.rows.get(0));
						octree.insert(tmpPage.rows.get(0).get(getClusteringKeyName(table.strTableName)), c - 1, tmpPage.rows.get(0));
						
						Octree.writeToFile(octree);
					}
					
					page.rows.add(page.rows.size(), tmpPage.rows.remove(0));
					
					if(page.rows.size() == maxTuplesPerPage)
						break;
				}
				
				Page.writeToFile(tmpPage, table.strTableName, c);
				c++;
			}
			
			if(!page.rows.isEmpty())
			{	
				page.minValue = page.rows.get(0).get(getClusteringKeyName(table.strTableName));
				page.maxValue = page.rows.get((page.rows.size() - 1)).get(getClusteringKeyName(table.strTableName));
			}
			Page.writeToFile(page, table.strTableName, i);
		}
	}
	
	public static void cleanUp(Table table)
	{
		Page page;
		for(int i = table.numOfPages; i > 0; i--)
		{
			page = Page.readFromFile(table.strTableName, i);
			
			if(page.rows.isEmpty())
			{
				String pageName = table.strTableName + "_" + i;
				File file = new File("src/resources/docs/pages/" + pageName + ".ser");
				file.delete();
				table.numOfPages--;
			}
		}
	}
	
	public boolean indexInvalid(Table table, Hashtable<String, Object> htblColNameValue)
	{
		if(table.numOfIndices == 0)
			return true;
		
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) 
		{
			double numOfIndexedColumns = 0;
			String line;
			while ((line = br.readLine()) != null) 
			{
				String[] values = line.split(",");
				
				if(values.length == 8)
				{
					if (values[0].equals(table.strTableName) && htblColNameValue.containsKey(values[1].substring(1)) && !values[4].substring(1).equals("null"))
					{
						numOfIndexedColumns++;
					}
				}
			}
			
			if(numOfIndexedColumns / table.numOfIndices != 3)
			{
				return true;
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			return false;
		}
		return false;
	}
	
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException
	{
		try 
		{
			if(htblColNameValue.isEmpty())
				throw new DBAppException("Please input something to delete");
				
			validateEntry(strTableName, htblColNameValue, false);
			
			boolean clusteringKeyCheck = false;
			Hashtable<String, Object> clusteringKey = new Hashtable<String, Object>();
			
			if(htblColNameValue.get(getClusteringKeyName(strTableName)) != null)
			{
				clusteringKey.put((String) getClusteringKeyName(strTableName), htblColNameValue.get(getClusteringKeyName(strTableName)));
				clusteringKeyCheck = true;
			}
			
			Table table = Table.readFromFile(strTableName);
			Page page;
			boolean inBetween;
			
			if(indexInvalid(table, htblColNameValue))
			{
				for(int i = 1; i <= table.numOfPages; i++)
				{
					page = Page.readFromFile(strTableName, i);
					
					if(clusteringKeyCheck)
					{
						inBetween = check(strTableName, clusteringKey, page, false, 1);
						
						if(inBetween)
						{
							int pos = binarySearch2(strTableName, page, clusteringKey.get(getClusteringKeyName(strTableName)));
							if(pos == -1)
								throw new DBAppException("Can not delete record that does not exist");
							
							if(satisfies(page.rows.get(pos), htblColNameValue))
							{
								Octree octree;
								HashSet<String> indices = getIndicesOnTable(strTableName);
								
								for(String indexName : indices)
								{
									octree = Octree.readFromFile(indexName);
	
									octree.delete(htblColNameValue.get(getClusteringKeyName(strTableName)), page.rows.get(pos));
									
									Octree.writeToFile(octree);
								}
								
								page.rows.remove(pos);
							}
							
							if(!page.rows.isEmpty())
							{	
								page.minValue = page.rows.get(0).get(getClusteringKeyName(strTableName));
								page.maxValue = page.rows.get((page.rows.size() - 1)).get(getClusteringKeyName(strTableName));
							}
							Page.writeToFile(page, strTableName, i);
							break;
						}
					}
					
					else
					{
						for(int j = 0; j < page.rows.size(); j++)
						{
							if(satisfies(page.rows.get(j), htblColNameValue))
							{
								Octree octree;
								HashSet<String> indices = getIndicesOnTable(strTableName);
								
								for(String indexName : indices)
								{
									octree = Octree.readFromFile(indexName);
	
									octree.delete(page.rows.get(j).get(getClusteringKeyName(strTableName)), page.rows.get(j));
									
									Octree.writeToFile(octree);
								}
								
								page.rows.remove(j--);
							}
							
							if(!page.rows.isEmpty())
							{	
								page.minValue = page.rows.get(0).get(getClusteringKeyName(strTableName));
								page.maxValue = page.rows.get((page.rows.size() - 1)).get(getClusteringKeyName(strTableName));
							}
						}
						
						Page.writeToFile(page, strTableName, i);
					}
				}
			}
			else
			{
				HashSet<Object[]> primaryKeyAndPageNumber = new HashSet<Object[]>();
				HashSet<Object[]> tmp = new HashSet<Object[]>();
				HashSet<String> indices = getIndicesOnTable(strTableName);
				Octree octree;
				
				for(String indexName : indices)
				{
					octree = Octree.readFromFile(indexName);
	
					tmp = octree.getRowsToBeDeleted(htblColNameValue);
						
					for(Object[] x : tmp)
					{
						primaryKeyAndPageNumber.add(x);
					}
						
					Octree.writeToFile(octree);
				}
				
				Page tmpPage;
				for(Object[] i : primaryKeyAndPageNumber)
				{
					tmpPage = Page.readFromFile(strTableName, (int) i[1]);
					int pos =  binarySearch2(strTableName, tmpPage, (int) i[0]);
					for(String indexName : indices)
					{
						octree = Octree.readFromFile(indexName);
						
						if(pos != -1)
							octree.delete(i[0], tmpPage.rows.get(pos));
						
						Octree.writeToFile(octree);
					}
					
					if(pos != -1)
					tmpPage.rows.remove(pos);
					
					Page.writeToFile(tmpPage, strTableName, (int) i[1]);
				}
			}

			shiftUp(table);
			cleanUp(table);
			
			Table.writeToFile(table);
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException
	{
		HashSet<String> columnNames = new HashSet<String>();
		
		for(int i = 0; i < strarrColName.length; i++)
			columnNames.add(strarrColName[i]);

		if(columnNames.size() != 3)
			throw new DBAppException("Index must be created on 3 different columns");
		
		Table table = Table.readFromFile(strTableName);
		
		for(String colName : columnNames)
		{
			if(table.htblColNameType.get(colName) == null)
				throw new DBAppException("Index cannot be created on column that does not exist.");
		}
		
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) 
		{
			String line;
			Pair[] pairs = new Pair[3];
			int c = 0;
			
			while ((line = br.readLine()) != null) 
			{
				String[] values = line.split(",");
				if (values[0].equals(strTableName))
				{
					if(columnNames.contains(values[1].substring(1)))
					{
						if(!values[4].substring(1).equals("null"))
						{
							//throw new DBAppException("Index already exists");
						}
						
						if(values[2].substring(1).equals("java.lang.Integer"))
							pairs[c++] = new Pair(Integer.parseInt(values[6].substring(1)), Integer.parseInt(values[7].substring(1)));
						
						if(values[2].substring(1).equals("java.lang.Double"))
							pairs[c++] = new Pair(Double.parseDouble(values[6].substring(1)), Double.parseDouble(values[7].substring(1)));
						
						if(values[2].substring(1).equals("java.lang.String"))
							pairs[c++] = new Pair(values[6].substring(1), values[7].substring(1));
						
						if(values[2].substring(1).equals("java.util.Date"))
						{
							SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD");  
							Date date1 = dateFormat.parse(values[6].substring(1));
							Date date2 = dateFormat.parse(values[7].substring(1));
							
							pairs[c++] = new Pair(date1, date2);
						}
					}
				}
			}
			Octree octree = new Octree(table.strTableName, pairs[0], pairs[1], pairs[2], strarrColName);
			
			table.numOfIndices++;
			
			if(table.numOfPages > 0)
			{
				Page page;
				for(int i = 1; i <= table.numOfPages; i++)
				{
					page = Page.readFromFile(table.strTableName, i);
					
					for(int j = 0; j < page.rows.size(); j++)
					{
						octree.insert(page.rows.get(j).get(getClusteringKeyName(strTableName)), i, page.rows.get(j));
					}
				}
			}
			
			Octree.writeToFile(octree);
			Table.writeToFile(table);
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		catch (ParseException e) 
		{
			e.printStackTrace();
		}
	}

//	public static void main(String[] args) throws DBAppException, IOException 
//	{
//		DBApp dbapp = new DBApp();
//		
//		init();
//		
////----------------------------------------------------------------------------------------------------
////---------------------------------------CREATE TABLE & INDEX-----------------------------------------
////----------------------------------------------------------------------------------------------------
//		Hashtable<String, String> ht2 = new Hashtable<String, String>();
//		ht2.put("X", "java.lang.Integer");
//		ht2.put("Y", "java.lang.Integer");
//		ht2.put("Z", "java.lang.String");
//		ht2.put("A", "java.lang.Integer");
//		
//		Hashtable<String, String> htmin = new Hashtable<String, String>();
//		htmin.put("X", "0");
//		htmin.put("Y", "0");
//		htmin.put("Z", "AAAA");
//		htmin.put("A", "0");
//		
//		Hashtable<String, String> htmax = new Hashtable<String, String>();
//		htmax.put("X", "100");
//		htmax.put("Y", "200");
//		htmax.put("Z", "YYYY");
//		htmax.put("A", "300");
//		
////		dbapp.createTable("CityShop", "A", ht2, htmin, htmax);
//		
//		String[] s = {"Z", "Y", "X"};
////		dbapp.createIndex("CityShop", s);
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
//		
//		
////----------------------------------------------------------------------------------------------------
////--------------------------------------------INSERT--------------------------------------------------
////----------------------------------------------------------------------------------------------------
//		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//		htblColNameValue.put("X", 48);
//		htblColNameValue.put("Y", 104);
//		htblColNameValue.put("Z", "BBBB");
//		htblColNameValue.put("A", 3);
//		dbapp.insertIntoTable("CityShop", htblColNameValue);
//		
////		Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>();
////		htblColNameValue2.put("X", 1);
////		htblColNameValue2.put("Y", 102);
////		htblColNameValue2.put("Z", 152);
////		dbapp.insertIntoTable("CityShop", htblColNameValue2);
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
//		
//		
////----------------------------------------------------------------------------------------------------
////--------------------------------------------UPDATE--------------------------------------------------
////----------------------------------------------------------------------------------------------------
//		Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>();
//		htblColNameValue2.put("Z", 157);
////		dbapp.updateTable("CityShop", "3", htblColNameValue2);
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
//		
//		
////----------------------------------------------------------------------------------------------------
////--------------------------------------------DELETE--------------------------------------------------
////----------------------------------------------------------------------------------------------------	
//		Hashtable<String, Object> htblColNameValue3 = new Hashtable<String, Object>();
//		htblColNameValue3.put("X", 48);
//		htblColNameValue3.put("Y", 104);
//		htblColNameValue3.put("A", 5);
////		dbapp.deleteFromTable("CityShop", htblColNameValue3);
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
//
//		
////----------------------------------------------------------------------------------------------------
////-------------------------------------DISPLAY OCTREE & TABLE-----------------------------------------
////----------------------------------------------------------------------------------------------------		
//		Octree octree = Octree.readFromFile("CityShop_Z_Y_X_Index");
//		System.out.println(octree.node);
//		
//		Table table = Table.readFromFile("CityShop");
//		for(int i = 1; i <= table.numOfPages; i++)
//		{
//			Page page = Page.readFromFile("CityShop", i);
////			System.out.println(page.rows);
//		}
//		
////		for(int i = 1; i <= table.numOfPages; i++)
////		{
////			Page page = Page.readFromFile("CityShop", i);
////			System.out.println(page.minValue + " " + page.maxValue);
////		}
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
//		
//		
////----------------------------------------------------------------------------------------------------
////------------------------------------DISPLAY METADATA------------------------------------------------
////----------------------------------------------------------------------------------------------------
////		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) 
////		{
////			String line;
////			while ((line = br.readLine()) != null) {
////				String[] values = line.split(",");
////				System.out.println(Arrays.toString(values));
////			}
////		}
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
////----------------------------------------------------------------------------------------------------
//	}
}
