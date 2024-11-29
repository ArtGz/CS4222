import java.sql.*;
import java.util.Scanner;

public class Client {
   static final String DB_URL = System.getenv("JDBC_SERVER");
   static final String USER = System.getenv("JDBC_USER");
   static final String PASS = System.getenv("JDBC_PASS");

   public static void main(String[] args) {
      if (USER == null || DB_URL == null || PASS == null) {
         System.out.println("Login / Server info cannot be null!\nPlease setup System ENVIRONMENT variables");
         return;
      }

      Scanner scanner = new Scanner(System.in);

      System.out.println("Logging in as user: " + USER + " to server @" + DB_URL);
      // Open a connection
      try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
         int action = 1;
         
         while(action != 0){
            System.out.println("Enter action:\n1: Show all table info\n2: Add a musician\n3: Remove a musician\n4: Add an album\n5: Remove an album\n6: Add a song\n7: Remove a song\n0: Quit");
            action = scanner.nextInt();
            switch(action){
               case 1:
                  printAllData(conn);
                  break;
               case 0:
                  System.out.println("Goodbye");
                  break;
               default:
                  System.out.println("Invalid option, try again.");
            }
         }


         try {
            conn.close();
         } catch (Exception e) {
         }

         // Failed to connect to the database.
      } catch (SQLException e) {
         e.printStackTrace();
         scanner.close();
         return;
      }

      scanner.close();
   }

   // Method to print all data using existing jdbc connection
   static void printAllData(Connection conn) {
      Statement stmt = null;

      try{
         stmt = conn.createStatement();
      } catch (Exception e){
         e.printStackTrace();
         System.out.println("Failed to create statement to print all tables.\n");
         return;
      }

      try {
         ResultSet rs = stmt.executeQuery("SELECT * FROM Musician LEFT JOIN Address ON Musician.address = Address.location");
         System.out.println("\n\nMusicians Table");
         System.out.println("SSN | Name | Address | Phone");
         while (rs.next()) {
            System.out.println(rs.getString("ssn") + " | " + rs.getString("name") + " | " + rs.getString("address") + " | " + rs.getString("phoneNumber"));
         }
         rs.close();
      } catch (Exception e) {
         e.printStackTrace();
      }

      try {
         ResultSet rs = stmt.executeQuery("SELECT * FROM Address");
         System.out.println("\n\nAddresses Table");
         System.out.println("Address | Phone Number");
         while (rs.next()) {
            System.out.println(rs.getString("location") + " | " + rs.getString("phonenumber"));
         }
         rs.close();
      } catch (Exception e) {
         e.printStackTrace();
      }

      try {
         ResultSet rs = stmt.executeQuery("SELECT * FROM Album");
         System.out.println("\n\nAlbums Table");
         System.out.println("ID | Format | Title | Identifier | Copyright Date");
         while (rs.next()) {
            System.out.println(rs.getInt("id") + " | " + rs.getString("format") + " | " + rs.getString("title") + " | " + rs.getString("identifier") + " | " + rs.getDate("copyrightdate"));
         }
         rs.close();
      } catch (Exception e) {
         e.printStackTrace();
      }

      try {
         ResultSet rs = stmt.executeQuery("SELECT * FROM Song");
         System.out.println("\n\nSongs Table");
         System.out.println("Author | Title | Album ID");
         while (rs.next()) {
            System.out.println(rs.getString("author") + " | " + rs.getString("title") + " | " + rs.getInt("albumid"));
         }
         rs.close();
      } catch (Exception e) {
         e.printStackTrace();
      }

      try {
         ResultSet rs = stmt.executeQuery("SELECT * FROM Performs");
         System.out.println("\n\nPerformers Table");
         System.out.println("Musician ID | Song ID");
         while (rs.next()) {
            System.out.println(rs.getString("ssn") + " | " + rs.getString("author") + " - " + rs.getString("title"));
         }

         rs.close();
      } catch (Exception e) {
         e.printStackTrace();
      }

      try {
         ResultSet rs = stmt.executeQuery("SELECT * FROM Plays");
         System.out.println("\n\nPlays Table");
         System.out.println("Musician ID | Instrument ID");
         while (rs.next()) {
            System.out.println(rs.getString("ssn") + " | " + rs.getInt("instrumentid"));
         }
         rs.close();
      } catch (Exception e) {
         e.printStackTrace();
      }

      try {
         ResultSet rs = stmt.executeQuery("SELECT * FROM Produces");
         System.out.println("\n\nAlbum Producers Table");
         System.out.println("Musician ID | Album ID");
         while (rs.next()) {
            System.out.println(rs.getString("ssn") + " | " + rs.getInt("albumid"));
         }
         rs.close();
      } catch (Exception e) {
         e.printStackTrace();
      }

      // Close statement that retreived all info
      if(stmt != null){
         try{
            stmt.close();
         }catch (Exception e) {}
      }

      System.out.println("\n\n");
   }

}
