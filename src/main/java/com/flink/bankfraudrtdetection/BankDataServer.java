package main.java.com.flink.bankfraudrtdetection;
import java.util.Date;
import java.net.Socket;
import java.util.Random;
import java.io.FileReader;
import java.sql.Timestamp;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.io.BufferedReader;

public class BankDataServer {
  public static void main(String[] args) throws IOException {
    ServerSocket listener = new ServerSocket(9090);
    BufferedReader br = null;
    try {
      Socket socket = listener.accept();
      System.out.println("Got new connection: " + socket.toString());

      br = new BufferedReader(new FileReader("/Users/Workspace/flink/bank_data/bankflink1.txt"));

      try {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        String line;

        while ((line = br.readLine()) != null) {
          out.println(line);
          Thread.sleep(500);
        }

      } finally {
        socket.close();
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      listener.close();
      if (br != null)
        br.close();
    }
  }
}