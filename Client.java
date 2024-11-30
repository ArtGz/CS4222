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
            System.out.print(">> ");
            try {
               action = Integer.parseInt(scanner.nextLine());
            } catch (NumberFormatException e) {
               System.out.println("Enter an integer\n");
               continue;
            }
            switch(action){
               case 2:
                  insertMusician(scanner, conn);
                  break;
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

   public static boolean isValidSSN(String ssn, Connection conn) {
      if(!ssn.matches("\\d{3}-\\d{2}-\\d{4}")) {
         System.out.println("'" + ssn + "' is not a valid SSN\n");
         return false;
      }
      if(isDuplicateKey(ssn, "musician","ssn", conn)) {
         System.out.println("'" + ssn + "' is already in the database\n");
         return false;
      }
      return true;
   }

   public static boolean isDuplicateKey(String key, String table, String attribute, Connection conn) {
      String sql = "SELECT * FROM "+table+" WHERE "+attribute+" = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)){
         ps.setString(1, key);
         return ps.executeQuery().isBeforeFirst();
      } catch(SQLException e) {
         e.printStackTrace();
      }
      return false;
   }

   public static String getInputOnAttribute(Scanner scanner, String prompt, String attribute, boolean required) {
      while (true) {
         System.out.print(prompt + (required ? "*" : "") + "\n>> ");
         String input = scanner.nextLine().trim();
         if (!input.isEmpty() || !required) return input;
         System.out.println(attribute + " is a required field");
      }
   }

   public static void insertMusician(Scanner scanner, Connection conn) {
      System.out.println("Add a musician\nRequired field *");
      // Collect validated inputs
      String ssn = getInputOnAttribute(scanner, "Enter SSN (format: XXX-XX-XXXX)","SSN", true);
      while (!isValidSSN(ssn, conn)) {
         ssn = getInputOnAttribute(scanner, "Enter SSN (format: XXX-XX-XXXX)","SSN", true);
      }

      String name = getInputOnAttribute(scanner, "Enter name", "name", true);
      String location = getInputOnAttribute(scanner, "Enter address","location",  true);
      String phone = getInputOnAttribute(scanner, "Enter phone number", "phone number", true);

      boolean locationExists = isDuplicateKey(location, "address", "location", conn);
      boolean phoneNumberExists = isDuplicateKey(phone, "address", "phoneNumber", conn);
      if(locationExists && !phoneNumberExists) {
         System.out.println("The address already exists with a different phone number.\n");
         return;
      }
      if(!locationExists && phoneNumberExists) {
         System.out.println("The phone number already exists with a different address.\n");
         return;
      }
      boolean addressExists = locationExists && phoneNumberExists;
      try {
         // Start the transaction
         conn.setAutoCommit(false);
         String sql;

         // Insert new address if not found
         if(!addressExists) {
            sql = "INSERT INTO address (location, phoneNumber) VALUES (?, ?)";
            try (PreparedStatement insertAddress = conn.prepareStatement(sql)) {
               insertAddress.setString(1, location);
               insertAddress.setString(2, phone);
               insertAddress.execute();
            }
         }

         // Insert musician
         sql = "INSERT INTO musician (ssn, name, address) VALUES (?, ?, ?)";
         try (PreparedStatement insertMusician = conn.prepareStatement(sql)) {
            insertMusician.setString(1, ssn);
            insertMusician.setString(2, name);
            insertMusician.setString(3, location);
            insertMusician.execute();
         }

         // Commit the transaction if everything succeeds
         conn.commit();
         conn.setAutoCommit(true);
         System.out.println("Successfully inserted\n");
      } catch (Exception e) {
         try {
            // Roll back the transaction if any exception occurs
            conn.rollback();
         } catch (SQLException rollbackEx) {
            System.err.println("Rollback failed: " + rollbackEx.getMessage());
            rollbackEx.printStackTrace();
         }
         e.printStackTrace();
      } finally {
         try {
            conn.setAutoCommit(true);
         } catch (SQLException autoCommitEx) {
            System.err.println("Failed to reset auto-commit: " + autoCommitEx.getMessage());
         }
      }
   }
}