package engine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Scanner;
import java.util.Vector;

public class Octree implements Serializable {
	private static final long serialVersionUID = -2884629616491963411L;
	String strTableName;
	String indexName;
	Node node;
	Vector<String> colNames;
	boolean first = true;

	public Octree(String strTableName, Pair one, Pair two, Pair three, String[] colName) {
		this.colNames = new Vector<String>();
		this.strTableName = strTableName;
		indexName = "";

		for (String columnName : colName) {
			colNames.add(columnName);
			indexName = indexName + columnName + "_";
		}

		indexName += "Index";

		try {
			node = new Node(one, two, three);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			updateMetaData(strTableName);
		} catch (IOException e) {
			e.printStackTrace();
		}

		writeToFile(this);
	}

	public void updateMetaData(String strTableName) throws IOException {
		File meta = new File("src/resources/data/metadata.csv");
		ArrayList<String[]> newMetaData = new ArrayList<String[]>();
		try {
			Scanner inputStream = new Scanner(meta);
			while (inputStream.hasNextLine()) {
				String s = inputStream.nextLine();

				String[] splitted = s.split(", ");

				if (s.split(", ")[0].equals(strTableName) && colNames.contains(s.split(", ")[1])) {
					splitted[4] = indexName;
					splitted[5] = "Octree";
				}

				newMetaData.add(splitted);

			}
			inputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		File dir = new File("src/resources/data/metadata.csv");
		FileWriter fileWriter = new FileWriter(dir);

		BufferedWriter bw = new BufferedWriter(fileWriter);
		PrintWriter out = new PrintWriter(bw);

		for (String[] line : newMetaData) {
			for (int i = 0; i < line.length; i++) {
				out.print(line[i] + (i + 1 == line.length ? "" : ", "));
			}
			out.println();
		}

		out.flush();
		out.close();
		fileWriter.close();
	}

	public static void writeToFile(Octree octree) {
		try {
			File file = new File("src/resources/docs/octrees/" + octree.strTableName + "_" + octree.indexName + ".ser");
			FileOutputStream out = new FileOutputStream(
					"src/resources/docs/octrees/" + octree.strTableName + "_" + octree.indexName + ".ser");
			ObjectOutputStream oos = new ObjectOutputStream(out);
			oos.writeObject(octree);
			out.close();
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Octree readFromFile(String indexName) {
		try {
			FileInputStream fileIn = new FileInputStream("src/resources/docs/octrees/" + indexName + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Octree octree = (Octree) in.readObject();
			in.close();
			fileIn.close();
			return octree;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public int getOctant(Node currentNode, Hashtable<String, Object> htblColNameValue) {
		Pair rangeX;
		if (first)
			rangeX = getMinAndMax(colNames.get(0));
		else
			rangeX = currentNode.x;
		Object midX = getMid(rangeX.a, rangeX.b);
		Object x = htblColNameValue.get(colNames.get(0));

		Pair rangeY = getMinAndMax(colNames.get(1));
		if (first)
			rangeY = getMinAndMax(colNames.get(1));
		else
			rangeY = currentNode.y;
		Object midY = getMid(rangeY.a, rangeY.b);
		Object y = htblColNameValue.get(colNames.get(1));

		Pair rangeZ = getMinAndMax(colNames.get(2));
		if (first)
			rangeZ = getMinAndMax(colNames.get(2));
		else
			rangeZ = currentNode.z;
		Object midZ = getMid(rangeZ.a, rangeZ.b);
		Object z = htblColNameValue.get(colNames.get(2));

		String pos = "";

		if (check(x, midX))
			pos = pos + "0";
		else
			pos = pos + "1";

		if (check(y, midY))
			pos = pos + "0";
		else
			pos = pos + "1";

		if (check(z, midZ))
			pos = pos + "0";
		else
			pos = pos + "1";

		int res = binaryToInteger(pos);

		first = false;

		return res;
	}

	public static int binaryToInteger(String binary) {
		char[] numbers = binary.toCharArray();
		int result = 0;

		for (int i = numbers.length - 1; i >= 0; i--)
			if (numbers[i] == '1')
				result += Math.pow(2, (numbers.length - i - 1));

		return result;
	}

	public Pair getMinAndMax(String colName) {
		try (BufferedReader br = new BufferedReader(new FileReader("src/resources/data/metadata.csv"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				if (values[0].equals(strTableName) && values[1].substring(1).equals(colName)) {
					if (values[2].substring(1).equals("java.lang.Integer"))
						return new Pair(Integer.parseInt(values[6].substring(1)),
								Integer.parseInt(values[7].substring(1)));

					if (values[2].substring(1).equals("java.lang.Double"))
						return new Pair(Double.parseDouble(values[6].substring(1)),
								Double.parseDouble(values[7].substring(1)));

					if (values[2].substring(1).equals("java.lang.String"))
						return new Pair(values[6].substring(1), values[7].substring(1));

					if (values[2].substring(1).equals("java.util.Date")) {
						SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD");
						Date date1 = dateFormat.parse(values[6].substring(1));
						Date date2 = dateFormat.parse(values[7].substring(1));

						return new Pair(date1, date2);
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return null;
	}

	public boolean check(Object input, Object mid) {
		if (input instanceof Integer) {
			if ((int) input <= (int) mid)
				return true;
			return false;
		}

		if (input instanceof Double) {
			if ((double) input <= (double) mid)
				return true;
			return false;
		}

		if (input instanceof String) {
			if (((String) input).compareToIgnoreCase((String) mid) <= 0)
				return true;
			return false;
		}

		if (input instanceof Date) {
			if (((Date) input).compareTo((Date) mid) <= 0)
				return true;
			return false;
		}

		return false;
	}

	public Object getMid(Object min, Object max) {
		if (min instanceof Integer) {
			return (int) ((int) min + (int) max) / 2;
		}

		if (min instanceof Double) {
			return ((double) min + (double) max) / 2;
		}

		if (min instanceof String) {
			return getMiddleString(((String) min).toLowerCase(), ((String) max).toLowerCase());
		}

		if (min instanceof Date) {
			long midwayTime = (((Date) min).getTime() + ((Date) max).getTime()) / 2;
			Date midwayDate = new Date(midwayTime);
			return midwayDate;
		}

		return null;
	}

	public static String getMiddleString(String S, String T) {
		String result = "";
		int N = S.length() > T.length() ? S.length() : T.length();

		if (S.length() != T.length()) {
			if (S.length() > T.length()) {
				T = T + S.substring(T.length());
			} else {
				S = S + T.substring(S.length());
			}
		}

		int[] a1 = new int[N + 1];

		for (int i = 0; i < N; i++) {
			a1[i + 1] = (int) S.charAt(i) - 97 + (int) T.charAt(i) - 97;
		}

		for (int i = N; i >= 1; i--) {
			a1[i - 1] += (int) a1[i] / 26;
			a1[i] %= 26;
		}

		for (int i = 0; i <= N; i++) {
			if ((a1[i] & 1) != 0) {
				if (i + 1 <= N) {
					a1[i + 1] += 26;
				}
			}

			a1[i] = (int) a1[i] / 2;
		}

		for (int i = 1; i <= N; i++) {
			result += (char) (a1[i] + 97);
		}

		return result;
	}

	public Node findCorrectNode(Hashtable<String, Object> htblColNameValue) {
		this.first = true;
		int x = this.getOctant(null, htblColNameValue);
		Node currentNode = this.node.children[x];

		while (currentNode.children != null) {
			x = this.getOctant(currentNode, htblColNameValue);
			currentNode = currentNode.children[x];
		}

		return currentNode;
	}

	public Hashtable<String, Object> getRows(Hashtable<String, Object> htblColNameValue) {
		Hashtable<String, Object> rowsIndexed = new Hashtable<String, Object>();
		rowsIndexed.put(colNames.get(0), htblColNameValue.get(colNames.get(0)));
		rowsIndexed.put(colNames.get(1), htblColNameValue.get(colNames.get(1)));
		rowsIndexed.put(colNames.get(2), htblColNameValue.get(colNames.get(2)));

		return rowsIndexed;
	}

	public boolean allIntoOne(Node node) {
		for (int i = 0; i < node.children.length; i++) {
			if (node.children[i].data.size() > Node.maxEntriesInOctreeNode) {
				return true;
			}
		}

		return false;
	}

	public Node getOverflowingNode(Node node) {
		for (int i = 0; i < node.children.length; i++) {
			if (node.children[i].data.size() > Node.maxEntriesInOctreeNode) {
				return node.children[i];
			}
		}

		return null;
	}

	public void split(Node node) throws IOException {
		Vector<Object[]> dataToBeMoved = new Vector<Object[]>();
		Vector<Vector<Object[]>> duplicatesToBeMoved = new Vector<Vector<Object[]>>();

		while (!node.data.isEmpty())
			dataToBeMoved.add(node.data.remove(0));

		while (!node.duplicates.isEmpty())
			duplicatesToBeMoved.add(node.duplicates.remove(0));

		node.initialiseChildren();

		Node tmp;

		for (int i = 0; i < dataToBeMoved.size(); i++) {
			tmp = findCorrectNode((Hashtable<String, Object>) dataToBeMoved.get(i)[2]);
			tmp.data.add(dataToBeMoved.get(i));

			for (int j = 0; j < duplicatesToBeMoved.size(); j++) {
				if (duplicatesToBeMoved.get(j).get(0)[2].equals(dataToBeMoved.get(i)[2])) {
					tmp.duplicates.add(duplicatesToBeMoved.get(j));
					break;
				}
			}
		}
		if (allIntoOne(node)) {
			tmp = getOverflowingNode(node);
			split(tmp);
		}
	}

	public void insert(Object primaryKey, int pageNum, Hashtable<String, Object> htblColNameValue) throws IOException {
		Node correctNode = findCorrectNode(htblColNameValue);
		Hashtable<String, Object> rowsIndexed = this.getRows(htblColNameValue);
		Object[] input = { primaryKey, pageNum, rowsIndexed };

		for (int i = 0; i < correctNode.data.size(); i++) {
			if (correctNode.data.get(i)[2].equals(rowsIndexed)) {
				boolean inserted = false;
				for (int j = 0; j < correctNode.duplicates.size(); j++) {
					for (int k = 0; k < correctNode.duplicates.get(j).size(); k++) {
						if (correctNode.duplicates.get(j).get(k)[2].equals(rowsIndexed)) {
							correctNode.duplicates.get(j).add(input);
							inserted = true;
							break;
						}
					}
				}

				if (!inserted) {
					Vector<Object[]> tmp = new Vector<Object[]>();
					tmp.add(input);
					correctNode.duplicates.add(tmp);
				}

				return;
			}
		}

		if (correctNode.data.size() > Node.maxEntriesInOctreeNode - 1) {
			Vector<Object[]> dataToBeMoved = new Vector<Object[]>();
			Vector<Vector<Object[]>> duplicatesToBeMoved = new Vector<Vector<Object[]>>();

			while (!correctNode.data.isEmpty())
				dataToBeMoved.add(correctNode.data.remove(0));

			while (!correctNode.duplicates.isEmpty())
				duplicatesToBeMoved.add(correctNode.duplicates.remove(0));

			correctNode.initialiseChildren();

			Node tmp;

			for (int i = 0; i < dataToBeMoved.size(); i++) {
				tmp = findCorrectNode((Hashtable<String, Object>) dataToBeMoved.get(i)[2]);
				tmp.data.add(dataToBeMoved.get(i));

				for (int j = 0; j < duplicatesToBeMoved.size(); j++) {
					if (!duplicatesToBeMoved.get(j).isEmpty()) {
						if (duplicatesToBeMoved.get(j).get(0)[2].equals(dataToBeMoved.get(i)[2])) {
							tmp.duplicates.add(duplicatesToBeMoved.get(j));
							break;
						}
					}
				}
			}
			tmp = findCorrectNode(htblColNameValue);
			tmp.data.add(input);

			if (allIntoOne(correctNode)) {
				tmp = getOverflowingNode(correctNode);
				split(tmp);
			}
		} else {
			correctNode.data.add(input);
		}
	}

	public void update(Object clusteringKeyValue, Hashtable<String, Object> oldValue,
			Hashtable<String, Object> newValue) throws IOException {
		Node node = findCorrectNode(oldValue);
		Hashtable<String, Object> rowsIndexed = this.getRows(oldValue);
		int pageNum = 0;

		if (newValue.size() < oldValue.size()) {
			for (String colName : oldValue.keySet()) {
				if (!newValue.containsKey(colName)) {
					newValue.put(colName, oldValue.get(colName));
				}
			}
		}

		for (int i = 0; i < node.data.size(); i++) {
			if (node.data.get(i)[2].equals(rowsIndexed) && clusteringKeyValue.equals(node.data.get(i)[0])) {
				pageNum = (int) node.data.get(i)[1];
				node.data.remove(i);
				this.insert(clusteringKeyValue, pageNum, newValue);
				for (int j = 0; j < node.duplicates.size(); j++) {
					if (!node.duplicates.get(j).isEmpty()) {
						if (node.duplicates.get(j).get(0)[2].equals(rowsIndexed)) {
							this.insert(node.duplicates.get(j).get(0)[0], (int) node.duplicates.get(j).get(0)[1],
									(Hashtable<String, Object>) node.duplicates.get(j).get(0)[2]);
							node.duplicates.get(j).remove(0);
						}
					}
				}
				return;
			}
		}

		for (int i = 0; i < node.duplicates.size(); i++) {
			if (node.duplicates.get(i).get(0)[2].equals(rowsIndexed)) {
				for (int j = 0; j < node.duplicates.get(i).size(); j++) {
					if (node.duplicates.get(i).get(j)[2].equals(rowsIndexed)
							&& clusteringKeyValue.equals(node.duplicates.get(i).get(j)[0])) {
						pageNum = (int) node.duplicates.get(i).get(j)[1];
						node.duplicates.get(i).remove(j);
						this.insert(clusteringKeyValue, pageNum, newValue);
						break;
					}
				}
				break;
			}
		}
	}

	public void delete(Object clusteringKeyValue, Hashtable<String, Object> htblColNameValue) {
		Node node = findCorrectNode(htblColNameValue);
		Hashtable<String, Object> rowsIndexed = this.getRows(htblColNameValue);

		for (int i = 0; i < node.data.size(); i++) {
			if (node.data.get(i)[2].equals(rowsIndexed) && clusteringKeyValue.equals(node.data.get(i)[0])) {
				node.data.remove(i--);
			}
		}

		for (int i = 0; i < node.duplicates.size(); i++) {
			if (!node.duplicates.get(i).isEmpty()) {
				if (node.duplicates.get(i).get(0)[2].equals(rowsIndexed))
					for (int j = 0; j < node.duplicates.get(i).size(); j++)
						if (node.duplicates.get(i).get(j)[2].equals(rowsIndexed)
								&& clusteringKeyValue.equals(node.duplicates.get(i).get(j)[0]))
							node.duplicates.get(i).remove(j--);
			}
		}
	}

	public HashSet<Object[]> getRowsToBeDeleted(Hashtable<String, Object> htblColNameValue) {
		Node node = findCorrectNode(htblColNameValue);
		Hashtable<String, Object> rowsIndexed = this.getRows(htblColNameValue);
		HashSet<Object[]> primaryKeyAndPageNumber = new HashSet<Object[]>();

		for (int i = 0; i < node.data.size(); i++) {
			if (node.data.get(i)[2].equals(rowsIndexed)) {
				Object[] tmp = { node.data.get(i)[0], node.data.get(i)[1] };
				primaryKeyAndPageNumber.add(tmp);
			}
		}

		for (int i = 0; i < node.duplicates.size(); i++) {
			if (!node.duplicates.get(i).isEmpty()) {
				if (node.duplicates.get(i).get(0)[2].equals(rowsIndexed)) {
					for (int j = 0; j < node.duplicates.get(i).size(); j++) {
						if (node.duplicates.get(i).get(j)[2].equals(rowsIndexed)) {
							Object[] tmp = { node.duplicates.get(i).get(j)[0], node.duplicates.get(i).get(j)[1] };
							primaryKeyAndPageNumber.add(tmp);
						}
					}
				}
			}
		}

		return primaryKeyAndPageNumber;
	}
}
