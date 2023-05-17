package engine;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Vector;

public class Node implements Serializable {
	private static final long serialVersionUID = 38320723521294739L;
	Vector<Object[]> data;
	Vector<Vector<Object[]>> duplicates;
	Pair x;
	Pair y;
	Pair z;
	Node[] children;
	static int maxEntriesInOctreeNode;

	private Node() throws IOException {
		data = new Vector<Object[]>();
		duplicates = new Vector<Vector<Object[]>>();
		x = new Pair();
		y = new Pair();
		z = new Pair();

		Properties properties = new Properties();
		FileInputStream fis = new FileInputStream("src/resources/config/DBApp.config");
		properties.load(fis);
		maxEntriesInOctreeNode = Integer.parseInt(properties.getProperty("MaximumEntriesinOctreeNode"));
	}

	public Node(Pair x, Pair y, Pair z) throws IOException {
		Properties properties = new Properties();
		FileInputStream fis = new FileInputStream("src/resources/config/DBApp.config");
		properties.load(fis);
		maxEntriesInOctreeNode = Integer.parseInt(properties.getProperty("MaximumEntriesinOctreeNode"));

		data = new Vector<Object[]>();
		duplicates = new Vector<Vector<Object[]>>();
		children = new Node[8];

		for (int i = 0; i < children.length; i++) {
			children[i] = new Node();

			if (i < children.length / 2)
				children[i].x = new Pair(x.a, this.getMid(x));
			else
				children[i].x = new Pair(this.getMid(x), x.b);

			if (i < 2 || (i > 3 && i < 6))
				children[i].y = new Pair(y.a, this.getMid(y));
			else
				children[i].y = new Pair(this.getMid(y), y.b);

			if (i % 2 == 0)
				children[i].z = new Pair(z.a, this.getMid(z));
			else
				children[i].z = new Pair(this.getMid(z), z.b);
		}
	}

	public void initialiseChildren() throws IOException {
		this.children = new Node[8];
		for (int i = 0; i < this.children.length; i++) {
			this.children[i] = new Node();

			if (i < this.children.length / 2)
				this.children[i].x = new Pair(this.x.a, this.getMid(this.x));
			else
				this.children[i].x = new Pair(this.getMid(this.x), this.x.b);

			if (i < 2 || (i > 3 && i < 6))
				this.children[i].y = new Pair(this.y.a, this.getMid(this.y));
			else
				this.children[i].y = new Pair(this.getMid(this.y), this.y.b);

			if (i % 2 == 0)
				this.children[i].z = new Pair(this.z.a, this.getMid(this.z));
			else
				this.children[i].z = new Pair(this.getMid(z), this.z.b);
		}
	}

	public Object getMid(Pair x) {
		Object mid = 0;

		if (x.a instanceof Integer)
			mid = (int) ((int) x.a + (int) x.b) / 2;

		if (x.a instanceof Double)
			mid = ((double) x.a + (double) x.b) / 2;

		if (x.a instanceof String) {
			mid = getMiddleString((String) x.a, (String) x.b);
		}

		if (x.a instanceof Date) {
			long midwayTime = (((Date) x.a).getTime() + ((Date) x.b).getTime()) / 2;
			Date midwayDate = new Date(midwayTime);
			return midwayDate;
		}

		return mid;
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

	public Node[] getChildren() {
		return children;
	}

	public String toString() {
		return "Data: " + this.dataInData() + '\n' + "Duplicates: " + this.duplicates() + '\n' + "Z: " + x + "Y: " + y
				+ "X: " + z + "Children: " + Arrays.toString(children) + '\n' + '\n';
	}

	public String duplicates() {
		String s = "";

		for (int i = 0; i < this.duplicates.size(); i++) {
			for (int j = 0; j < duplicates.get(i).size(); j++) {
				s = s + " " + duplicates.get(i).get(j)[0] + " " + duplicates.get(i).get(j)[1] + " "
						+ duplicates.get(i).get(j)[2];
			}
		}

		return s;
	}

	public String dataInData() {
		String s = "";
		for (int i = 0; i < this.data.size(); i++) {
			s = s + " " + data.get(i)[0] + " " + data.get(i)[1] + " " + data.get(i)[2];
		}

		return s;
	}
}
