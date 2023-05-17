package engine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

public class Page implements java.io.Serializable {
	private static final long serialVersionUID = 2638310056538136664L;
	Vector<Hashtable<String, Object>> rows = new Vector<Hashtable<String, Object>>();
	Object minValue = 0;
	Object maxValue = 0;
	
	public boolean equals(Page page)
	{
		if(this.rows.equals(page.rows))
			return true;
		return false;
	}
	
	public static void writeToFile(Page page, String strTableName, int pageNum)
	{
		String pageName = strTableName + "_" + pageNum;
		try {
			File dir = new File("src/resources/docs/pages/" + pageName + ".ser");
			FileOutputStream out = new FileOutputStream("src/resources/docs/pages/" + pageName + ".ser");
			ObjectOutputStream oos = new ObjectOutputStream(out);
			oos.writeObject(page);
			out.close();
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Page readFromFile(String strTableName, int pageNum)
	{
		String pageName = strTableName + "_" + pageNum;
		if(doesPageExist(strTableName))
		{
			try {
				FileInputStream fileIn = new FileInputStream("src/resources/docs/pages/" + pageName + ".ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				Page page = (Page) in.readObject();
				in.close();
				fileIn.close();
				return page;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public static boolean doesPageExist(String pageName) {
		File file = new File("src/resources/docs/pages/");
		String[] pages = file.list();

		for (String page : pages)
			if (page.startsWith(pageName))
				return true;

		return false;
	}
}
