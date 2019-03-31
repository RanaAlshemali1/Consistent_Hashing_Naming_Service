import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Scanner;


public class NameServer {

	public static final String LOOKUP_COMMAND = "lookup";
	public static final String INSERT_COMMAND = "insert";
	public static final String DELETE_COMMAND = "delete";

	public static final String ENTER_COMMAND = "enter";
	public static final String EXIT_COMMAND = "exit";  
	public static final String PRED_EXIT_COMMAND = "predExit";  
	public static final String SUCC_EXIT_COMMAND = "succExit";  
	
	public static final String ASK_RANGE_COMMAND = "askRange";
	public static final String GET_DATA_COMMAND = "getData";
	public static final String UPDATE_INFO_COMMAND = "updateInfo";

	public static final String KEY_NOT_FOUND_MESSAGE = "Key Not Found";
	public static final String SUCCESSFUL_DELETION_MESSAGE = "Successful Deletion";
	public static final String SUCCESSFUL_ENTERY_MESSAGE = "Successful Entry";
	public static final String SUCCESSFUL_EXIT_MESSAGE = "Successful Exit";

	private String btnsIP;
	private int btnsPort;

	private static int[] dataRange = new int[2];
	private static HashMap<Integer, String> data = new HashMap<Integer, String>();

	private static int myID, predID, succID;
	private static String myIP, bnsIP, predIP, succIP;
	private static int myPort, bnsPort, predPort, succPort;
	private static ServerSocket serverSocket;
	private static Socket socket;

	public static void main(String[] args) throws IOException {

		String fileName = args[0];
		FileInputStream fstream = new FileInputStream(fileName);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream)); 

		myIP = "localhost";
		myID = Integer.parseInt(br.readLine());
		myPort = Integer.parseInt(br.readLine());
		String bsnsInfo = br.readLine().toString();
		String[] bsnsInfoSplit = bsnsInfo.split(" ");
		bnsIP = bsnsInfoSplit[0];
		bnsPort = Integer.parseInt(bsnsInfoSplit[1]);

		System.out.println("ID = " + myID + ", Port = " + myPort); 
		System.out.println("bnsIP = " + bnsIP + ", bnsPort = " + bnsPort); 

		serverSocket = new ServerSocket(myPort);

		Thread ThreadA = new ThreadA();
		ThreadA.start();

		Thread ThreadB = new ThreadB();
		ThreadB.start();

	}

	// for enter, exit from command line
	// make connection to bns to get position
	// meke connection to succ to take data
	static class ThreadA extends Thread {   


		private Scanner scanner;

		public ThreadA() {

		}

		@Override
		public void run() {
			//System.out.println("ThreadA listening for enter / exit .."); 

			while (true) {
				scanner = new Scanner(System.in);  
				String command = null; 
				System.out.print("\n> ");
				command = scanner.nextLine();

				switch (command) {
				case ENTER_COMMAND:
					enter();
					//System.out.println("NS Entry: succID: " + succID + ", predID: " + predID + ", succIP: " + succIP + ", predIP: " + predIP);
					//System.out.println("NS Entry: succPort: " + succPort + ", predPort: " + predPort + ", startRange: " + dataRange[0] + ", endRange: " + dataRange[1]);
					break;
				case EXIT_COMMAND:
					exit();
					//System.out.println("NS Exit: succID: " + succID + ", predID: " + predID + ", succIP: " + succIP + ", predIP: " + predIP);
					//System.out.println("NS Exit: succPort: " + succPort + ", predPort: " + predPort + ", startRange: " + dataRange[0] + ", endRange: " + dataRange[1]);
					//System.out.println("NS Exit: dstsLength: " + data.size());
					break;
				default:
					//dos.writeUTF(INVALID_INPUT);
					break;
				}
			}
		}

		private void enter() {
			// TODO Auto-generated method stub
			try {
				Socket socket = new Socket (bnsIP, bnsPort);
				DataInputStream dis = new DataInputStream(socket.getInputStream());
				DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

				// pass id, ip, port
				dos.writeUTF(ENTER_COMMAND);
				dos.writeInt(myID);
				dos.writeUTF(myIP);
				dos.writeInt(myPort);

				// get succID, predID
				succID = dis.readInt();
				predID = dis.readInt();
				// get succIP, predIP
				succIP = dis.readUTF();
				predIP = dis.readUTF();
				// get succPort, predPort
				succPort = dis.readInt();
				predPort = dis.readInt();
				// get startRange, endRange
				dataRange[0] = dis.readInt();
				dataRange[1] = dis.readInt();


				String IDs = dis.readUTF();

				socket.close();
				dis.close();
				dos.close();


				getDataEntery(); 
				updateNewPred();

				System.out.println("dataLength: " + data.size()); 

				System.out.println("---------------------------"); 
				System.out.println(SUCCESSFUL_ENTERY_MESSAGE); 
				System.out.println("1) Range: [" + dataRange[0] + "-" + dataRange[1]+"]");  				
				System.out.println("2) succID: " + succID + ", predID: " + predID); 
				System.out.println("3) Traversed Servers:"); 
				System.out.print(IDs);  


			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
		private void updateNewPred() {
			// TODO Auto-generated method stub
			try {
				Socket socket = new Socket (predIP, predPort);
				DataInputStream dis = new DataInputStream(socket.getInputStream());
				DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

				dos.writeUTF(UPDATE_INFO_COMMAND);
				dos.writeInt(myID);
				dos.writeUTF(myIP);
				dos.writeInt(myPort);

				socket.close();
				dis.close();
				dos.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		private void getDataEntery() {
			// TODO Auto-generated method stub
			try{
				//System.out.println("**** succIP: " + succIP + ", succPort: " + succPort); 
				Socket socket = new Socket (succIP, succPort);
				DataInputStream dis = new DataInputStream(socket.getInputStream());
				DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
				System.out.println("Getting Data .."); 
				dos.writeUTF(GET_DATA_COMMAND);
				// send range
				dos.writeInt(dataRange[0]);
				dos.writeInt(dataRange[1]);

				// send new info
				dos.writeInt(myID);
				dos.writeUTF(myIP);
				dos.writeInt(myPort);

				// get data 
				String getData = dis.readUTF();
				if(!getData.equals("")) {
					String[] splitData = getData.split(" ");
					//System.out.println("splitData: " + splitData.length/2); 
					int key;
					String value;
					for(int i = 0; i < splitData.length ; i++) {
						key = Integer.valueOf(splitData[i]);
						i++;
						value = splitData[i];
						data.put(key, value);
					}
				}

				printData();

				socket.close();
				dis.close();
				dos.close();

			}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		private void exit() {
			// TODO Auto-generated method stub  
				// inform successor
				inforSuccessorExit(); 
				// inform predecessor
				inforPredecessorExit();
				System.out.println("---------------------------"); 
				System.out.println(SUCCESSFUL_EXIT_MESSAGE); 
				System.out.println("1) succID: " + succID + ", predID: " + predID); 
				System.out.println("2) Range Handed: [" + dataRange[0] + "-" + dataRange[1]+"]");  
				
				succID = predID = succPort = predPort = dataRange[0] = dataRange[1] = -1;
				succIP = predIP = null;
				
		}
		
		private void inforPredecessorExit() {
			try {
				// inform successor 
				Socket socket = new Socket (predIP, predPort);
				DataInputStream dis = new DataInputStream(socket.getInputStream());
				DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
				
				// pass id, ip, port
				dos.writeUTF(SUCC_EXIT_COMMAND); 
				
				dos.writeInt(succID);
				dos.writeUTF(succIP);
				dos.writeInt(succPort);
				  
				socket.close();
				dis.close();
				dos.close();
				 
				
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		private void inforSuccessorExit() {
			try {
				// inform successor 
				Socket socket = new Socket (succIP, succPort);
				DataInputStream dis = new DataInputStream(socket.getInputStream());
				DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
				
				// pass id, ip, port
				dos.writeUTF(PRED_EXIT_COMMAND); 
				
				dos.writeInt(predID);
				dos.writeUTF(predIP);
				dos.writeInt(predPort);
				
				dos.writeInt(dataRange[0]); 
				
				// give Successor Data 
				String sendData = "";
				for(int i = dataRange[0]; i <= dataRange[1] ; i++) {
					if(data.containsKey(i)) {
						sendData+= i + " " + data.get(i) + " "; 
						data.remove(i);
					}
				}
				//System.out.println(sendData); 
				dos.writeUTF(sendData); 
				socket.close();
				dis.close();
				dos.close();
	
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		 
	}

	// for lookup / insertion / deletion from connection
	// listen to port: for incoming connections from pred
	// make other connections to succ 

	static class ThreadB extends Thread {   
		private Socket socket;
		private DataInputStream dis;
		private DataOutputStream dos;

		public ThreadB() {

		}

		@Override
		public void run() {
			//System.out.println("ThreadB listening for lookup / insertion / deletion ..");  

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
			case PRED_EXIT_COMMAND:
				System.out.println("Predecessor Exiting.."); 
				predExit();
				//System.out.println("1) NS Update: succID: " + succID + ", predID: " + predID + ", succIP: " + succIP + ", predIP: " + predIP);
				//System.out.println("1) NS Update: succPort: " + succPort + ", predPort: " + predPort + ", startRange: " + dataRange[0] + ", endRange: " + dataRange[1]);
				break;
			case SUCC_EXIT_COMMAND:
				System.out.println("Successor Exiting.."); 
				succExit();
				//System.out.println("2) NS Update: succID: " + succID + ", predID: " + predID + ", succIP: " + succIP + ", predIP: " + predIP);
				//System.out.println("2) NS Update: succPort: " + succPort + ", predPort: " + predPort + ", startRange: " + dataRange[0] + ", endRange: " + dataRange[1]);
				break;
			case ASK_RANGE_COMMAND:
				System.out.println("Checking Range .."); 
				askRange();
				break;
			case GET_DATA_COMMAND:
				System.out.println("Sending Data .."); 
				giveDataEntery();
				//System.out.println("3) NS Update: succID: " + succID + ", predID: " + predID + ", succIP: " + succIP + ", predIP: " + predIP);
				//System.out.println("3) NS Update: succPort: " + succPort + ", predPort: " + predPort + ", startRange: " + dataRange[0] + ", endRange: " + dataRange[1]);
				break;
			case UPDATE_INFO_COMMAND:
				System.out.println("Updating Info .."); 
				UpdateInfoEntery();
				//System.out.println("4) NS Update: succID: " + succID + ", predID: " + predID + ", succIP: " + succIP + ", predIP: " + predIP);
				//System.out.println("4) NS Update: succPort: " + succPort + ", predPort: " + predPort + ", startRange: " + dataRange[0] + ", endRange: " + dataRange[1]);
				break;
			default:
				//dos.writeUTF(INVALID_INPUT);
				break;
			}

			socket.close();
			dis.close();
			dos.close();
		}

		private void predExit() {
			try {
				predID = dis.readInt();
				predIP = dis.readUTF();
				predPort = dis.readInt();
				dataRange[0] = dis.readInt();  
				
				// get data 
				String getData = dis.readUTF();
				if(!getData.equals("")) {
					String[] splitData = getData.split(" ");
					//System.out.println("splitData: " + splitData.length/2); 
					int key;
					String value;
					for(int i = 0; i < splitData.length ; i++) {
						key = Integer.valueOf(splitData[i]);
						i++;
						value = splitData[i];
						data.put(key, value);
					}
				}
				
				printData();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		private void succExit() {
			try {
				succID = dis.readInt();
				succIP = dis.readUTF();
				succPort = dis.readInt(); 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		private void askRange() {
			// TODO Auto-generated method stub
			try {
				int range = dis.readInt();
				if(range > dataRange[0] && range <= dataRange[1]) {
					//System.out.println("Found ..");
					dos.writeBoolean(true);
					dos.writeInt(predID);
					dos.writeUTF(predIP);
					dos.writeInt(predPort);
				}else {
					//System.out.println("Not Found ..");
					dos.writeBoolean(false); 
					dos.writeInt(succID);
					dos.writeUTF(succIP);
					dos.writeInt(succPort);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
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
				//System.out.println(sendData); 
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
