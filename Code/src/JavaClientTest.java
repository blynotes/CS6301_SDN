// By Greg Ozbirn, University of Texas at Dallas
// Adapted from example at Sun website: 
// http://java.sun.com/developer/onlineTraining/Programming/BasicJava2/socket.html
// 11/07/07

import java.io.*;
import java.util.Scanner;
import java.net.*;

public class JavaClientTest
{
	Socket socket = null;
	PrintWriter out = null;
	BufferedReader in = null;
	
	public void communicate()
	{
		String name = "Test";

		//Send data over socket
		out.println(name);

		//Receive text from server
		try
		{
			String line = in.readLine();
			System.out.println("Text received: " + line);
		} 
		catch (IOException e)
		{
			System.out.println("Read failed");
			System.exit(1);
		}
	}
  
	public void listenSocket(String host, int port)
	{
		//Create socket connection
		try
		{
			socket = new Socket(host, port);
			out = new PrintWriter(socket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		} 
		catch (UnknownHostException e) 
		{
			System.out.println("Unknown host");
			System.exit(1);
		} 
		catch (IOException e) 
		{
			System.out.println("No I/O");
			System.exit(1);
		}
	}

	public static void main(String[] args)
	{
		JavaClientTest client = new JavaClientTest();

		String host = "localhost";
		int port = 5678;
		client.listenSocket(host, port);
		client.communicate();
	}
}
