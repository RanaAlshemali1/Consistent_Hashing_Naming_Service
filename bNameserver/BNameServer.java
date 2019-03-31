import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream; 
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Scanner;

public class BNameServer {

	public static final String LOOKUP_COMMAND = "lookup";
	public static final String INSERT_COMMAND = "insert";
	public static final String DELETE_COMMAND = "delete";

	public static final String ENTER_COMMAND = "enter";
	public static final String EXIT_COMMAND = "exit"; 
	public static final String ASK_RANGE_COMMAND = "askRange";
	public static final String GET_DATA_COMMAND = "getData"; 
	public static final String UPDATE_INFO_COMMAND = "updateInfo";

	public static final String KEY_NOT_FOUND_MESSAGE = "Key Not Found";
	public static final String SUCCESSFUL_DELETION_MESSAGE = "Successful Deletion";
	public static final String SUCCESSFUL_ENTERY_MESSAGE = "Successful Entry";
	public static final String SUCCESSFUL_EXIT_MESSAGE = "Successful Exit";

	private static int[] dataRange = {0, 1023};
	private static HashMap<Integer, String> data = new HashMap<Integer, String>();

	private static int myID, predID, succID;
	private static String myIP, predIP, succIP;
	private static int myPort, predPort, succPort; 
	private static ServerSocket serverSocket;

	public static void main(String[] args) throws IOException {

		String fileName = args[0];
		FileInputStream fstream = new FileInputStream(fileName);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream)); 

		myID = Integer.parseInt(br.readLine());
		myPort = Integer.parseInt(br.readLine());
		myIP = predIP = succIP = "localhost";
		predPort = succPort = myPort;
		predID = succID = myID;

		System.out.println("ID = " + myID + ", Port = " + myPort); 

		// fill the data hashmap
		String line = null; 
		while ((line = br.readLine()) != null) {
			String[] lineSplit = line.split(" ");
			int dataID = Integer.parseInt(lineSplit[0]);
			String dataValue = lineSplit[1];
			data.put(dataID, dataValue);
			System.out.println(dataID + " " + dataValue); 
		}  

		System.out.println("Bootstrap Name Server is running .."); 
		serverSocket = new ServerSocket(myPort);

		Thread ThreadA = new ThreadA();
		ThreadA.start();

		Thread ThreadB = new ThreadB();
		ThreadB.start();

	}

	// for lookup / insertion / deletion from command line
	// listen to port: for incoming connections from pred
	// make other connections to succ
	static class ThreadA extends Thread {  
		private Socket socket;
		private DataInputStream dis;
		private DataOutputStream dos;
		private Scanner scanner;


		public ThreadA() {

		}

		@Override
		public void run() {

			//System.out.println("ThreadA listening for lookup / insertion / deletion .."); 

			while (true) {
				scanner = new Scanner(System.in);  

				String command = null;
				String key = null;
				String value = null;
				int commandLength;

				command = scanner.nextLine();

				if (command.contains(" ")) {
					String[] splittedCommand = command.split(" ");
					commandLength = splittedCommand.length;
					command = splittedCommand[0];
					if (commandLength == 2)
						key = splittedCommand[1];
					if (commandLength == 3) 
						value = splittedCommand[2];
				}

				switch (command) {
				case LOOKUP_COMMAND:
					lookUp(key);
					break;
				case INSERT_COMMAND:
					insert(key, value);
					break;
				case DELETE_COMMAND:
					delete(key);
					break; 
				default:
					//dos.writeUTF(INVALID_INPUT);
					break;
				}
			}
		}

		private void lookUp(String key) {
			// TODO Auto-generated method stub
		}

		private void insert(String key, String value) {
			// TODO Auto-generated method stub
		}

		private void delete(String key) {
			// TODO Auto-generated method stub
		}
	}

	// for enter and exit from connection
	//
	static class ThreadB extends Thread {  
		private Socket socket;
		private DataInputStream dis;
		private DataOutputStream dos;

		public ThreadB() { 

		}

		@Override
		public void run() {
			//System.out.println("ThreadB listening for enter / exit .."); 

			try {
				while (true) {
					socket = null;
					socket = serverSocket.accept();
					// input and output streams
					dis = new DataInputStream(socket.getInputStream());
					dos = new DataOutputStream(socket.getOutputStream());
					startProccess();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} 
		private void startProccess() throws IOException {
			String command = null; 

			command = dis.readUTF();

			switch (command) {
			case ENTER_COMMAND:
				System.out.println("New connection is accepted .."); 
				enter();
				break;
			case EXIT_COMMAND:
				exit();
				break;
			case GET_DATA_COMMAND:
				System.out.println("Sending Data .."); 
				giveDataEntery();
				System.out.println("BNS Update: succID: " + succID + ", predID: " + predID + ", succIP: " + succIP + ", predIP: " + predIP);
				System.out.println("BNS Update: succPort: " + succPort + ", predPort: " + predPort + ", startRange: " + dataRange[0] + ", endRange: " + dataRange[1]);
				break;
			case UPDATE_INFO_COMMAND:
				System.out.println("Updating Info .."); 
				UpdateInfoEntery();
				System.out.println("BNS Update: succID: " + succID + ", predID: " + predID + ", succIP: " + succIP + ", predIP: " + predIP);
				System.out.println("BNS Update: succPort: " + succPort + ", predPort: " + predPort + ", startRange: " + dataRange[0] + ", endRange: " + dataRange[1]);
				break;
			default:
				//dos.writeUTF(INVALID_INPUT);
				break;
			}

			socket.close();
			dis.close();
			dos.close();
		}

		private void enter() {
			// TODO Auto-generated method stub
			int newID = 0, newPort = 0;
			String newIP = null;

			try {
				// get id, ip, port  
				newID = dis.readInt();
				newIP = dis.readUTF();
				newPort = dis.readInt();
			} catch (IOException e) {
				e.printStackTrace();
			}

			// 1) bns is the only one in the ring
			//if(myID == succID && myID == predID)
			if(dataRange[0] == 0 && dataRange[1] == 1023)
				enterOnlyBns(newID, newIP, newPort);
			else 
				findSpotNS(newID, newIP, newPort);
		}

		private void enterOnlyBns(int newID, String newIP, int newPort) { 
			try { 
				System.out.println("newID: " + newID + ", newIP: " + newIP + ", newPort: " + newPort);
				System.out.println("Bns is the only one present");

				int newStartRange = dataRange[0], newEndRange = newID - 1;
				succID = newID;
				predID = newID;
				succIP = newIP;
				predIP = newIP;
				succPort = newPort;
				predPort = newPort;
				dataRange[0] = newID;
				dataRange[1] = 1023;

				// give his succID, predID
				dos.writeInt(myID);
				dos.writeInt(myID);
				// give his succIP, predIP
				dos.writeUTF(myIP);
				dos.writeUTF(myIP);
				// give his succPort, predPort
				dos.writeInt(myPort);
				dos.writeInt(myPort);
				// give his startRange, endRange
				dos.writeInt(newStartRange);
				dos.writeInt(newEndRange);

				dos.writeUTF("   ID: "+ myID +"\n"); 

				// -------------------------------------------


			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void findSpotNS(int newID, String newIP, int newPort) {
			try { 
				System.out.println("newID: " + newID + ", newIP: " + newIP + ", newPort: " + newPort);
				System.out.println("Bns is looking for a spot ..");

				boolean found = false;
				String IDs;
				int newPredPort = 0, newSuccPort = 0, nextPort = succPort;
				String newPredIP = "", newSuccIP = "", nextIP = succIP;
				int newPredID = 0, newSuccID = 0, nextID = succID;

				IDs = "   ID: "+ myID +"\n";;
				int newStartRange = 0, newEndRange = 0;


				while(!found) {
					//System.out.println(found);
					//System.out.println("nextID: " + nextID +", nextIP: " + nextIP + ", nextPort: "+ nextPort);
					IDs += "   ID: "+ nextID +"\n";
					Socket tSocket = new Socket(nextIP, nextPort);
					DataInputStream tDis = new DataInputStream(tSocket.getInputStream());
					DataOutputStream tDos = new DataOutputStream(tSocket.getOutputStream());

					tDos.writeUTF(ASK_RANGE_COMMAND);
					tDos.writeInt(newID);
					found = tDis.readBoolean();

					if(found) {
						//System.out.println("Found ..");
						newSuccID =  nextID;
						newSuccIP = nextIP;
						newSuccPort = nextPort;

						newPredID = tDis.readInt();
						newPredIP = tDis.readUTF();
						newPredPort = tDis.readInt(); 

						tSocket.close();
						tDis.close();
						tDos.close();

						break;
					}
					else { 
						//System.out.println("Not Found ..");
						nextID = tDis.readInt();
						nextIP = tDis.readUTF();
						nextPort = tDis.readInt();
						//System.out.println("End Not Found ..");
					}

					if(nextID == myID) {

						newSuccID = myID;
						newSuccIP = myIP;
						newSuccPort = myPort;

						newPredID = predID;
						newPredIP = predIP;
						newPredPort = predPort; 

						tSocket.close();
						tDis.close();
						tDos.close();
						found = true;
					}
				}

				//System.out.println("1");
				if(!found) {
					System.out.println("Couldn't find a spot ..");
					return;
				}

				newStartRange = newPredID;
				newEndRange = newID - 1;

				// give his succID, predID
				dos.writeInt(newSuccID);
				dos.writeInt(newPredID);
				// give his succIP, predIP
				dos.writeUTF(newSuccIP);
				dos.writeUTF(newPredIP);
				// give his succPort, predPort
				dos.writeInt(newSuccPort);
				dos.writeInt(newPredPort);
				// give his startRange, endRange
				dos.writeInt(newStartRange);
				dos.writeInt(newEndRange);

				dos.writeUTF(IDs); 
				//System.out.println("2");

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void exit() {
			// TODO Auto-generated method stub

		}

		private void UpdateInfoEntery() {
			// TODO Auto-generated method stub
			try {
				succID = dis.readInt();
				succIP = dis.readUTF();
				succPort = dis.readInt();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void giveDataEntery() {
			// TODO Auto-generated method stub
			int[] newDataRange = new int[2];
			try {
				// get data range to send
				newDataRange[0] = dis.readInt();
				newDataRange[1] = dis.readInt();

				// get new pred info to update 
				predID = dis.readInt();
				predIP = dis.readUTF();
				predPort = dis.readInt();


				// Send data
				String sendData = "";
				for(int i = newDataRange[0]; i <= newDataRange[1] ; i++) {
					if(data.containsKey(i)) {
						sendData+= i + " " + data.get(i) + " ";
						//if(i< dataRange[1]-1)
						//sendData+= " ";
						data.remove(i);
					}
				}
				System.out.println(sendData); 
				dos.writeUTF(sendData);
				// new Range
				dataRange[0] = newDataRange[1] + 1;  

				printData();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static void printData() {
		for(int i = dataRange[0]; i <= dataRange[1] ; i++) {
			if(data.containsKey(i)) { 
				System.out.println("- " + i + " " + data.get(i)); 
			}
		}
	}
}
