package engine;

import java.io.Serializable;

public class Pair implements Serializable
{
	private static final long serialVersionUID = -7812538907173356056L;
	Object a;
	Object b;
	
	public Pair()
	{
		
	}
	
	public Pair (Object a, Object b)
	{
		this.a = a;
		this.b = b;
	}
	
	public String toString()
	{
		return a + " - " + b + '\n';
	}
}
