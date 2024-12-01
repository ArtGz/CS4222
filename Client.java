import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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
               case 7:
                  removeSong(scanner, conn);
                  break;
               case 6:
                  insertSong(scanner, conn);
                  break;
               case 5:
                  removeAlbum(scanner, conn);
                  break;
               case 4:
                  insertAlbum(scanner, conn);
                  break;
               case 3:
                  removeMusician(scanner, conn);
                  break;
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

   public static boolean isValidSSN(String ssn, Connection conn, Boolean checkForDuplicates) {
      if(!ssn.matches("\\d{3}-\\d{2}-\\d{4}")) {
         System.out.println("'" + ssn + "' is not a valid SSN\n");
         return false;
      }
      if(checkForDuplicates && isDuplicateKey(ssn, "musician","ssn", conn)) {
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
      System.out.println("\n\nAdd a musician\nRequired field *");
      // Collect validated inputs
      String ssn = getInputOnAttribute(scanner, "Enter SSN (format: XXX-XX-XXXX)","SSN", true);
      while (!isValidSSN(ssn, conn, true)) {
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

   public static void removeMusician(Scanner scanner, Connection conn){
      String queryString = "DELETE FROM musician WHERE ssn = ?";
      String ssn = getInputOnAttribute(scanner, "Enter SSN to remove (format: XXX-XX-XXXX)","SSN", true);
      while (!isValidSSN(ssn, conn, false)) {
         ssn = getInputOnAttribute(scanner, "Invalid SSN, try again (format: XXX-XX-XXXX)","SSN", true);
      }

      try{
         PreparedStatement deleteMusician = conn.prepareStatement(queryString);
         deleteMusician.setString(1, ssn);
         int deleted = deleteMusician.executeUpdate();
         System.out.println("Successfully executed deletion: Deleted " + deleted + " row(s)\n");
         deleteMusician.close();
      } catch (Exception e){
         System.out.println("Failed to delete musician.");
      }
   }

   public static void removeSong(Scanner scanner, Connection conn){
      String queryString = "DELETE FROM song WHERE author = ? AND title = ?";
      String author = getInputOnAttribute(scanner, "Enter author of song","author", true);
      String title = getInputOnAttribute(scanner, "Enter title of song","title", true);
      try {
         PreparedStatement deleteSong = conn.prepareStatement(queryString);
         deleteSong.setString(1, author);
         deleteSong.setString(2, title);
         int deleted = deleteSong.executeUpdate();
         System.out.println("Successfully executed deletion: Deleted " + deleted + " row(s)\n");
         deleteSong.close();
      } catch (Exception e){
         e.printStackTrace();
         System.out.println("Failed to delete song.\n");
      }
   }

   public static void removeAlbum(Scanner scanner, Connection conn) {
      String queryString = "DELETE FROM album WHERE id = ?";
      int id = -1;

      while (id == -1) {
         try {
            System.out.print("Enter an album id (integer)*\n>> ");
            id = Integer.parseInt(scanner.nextLine());
         } catch (NumberFormatException e) {
            System.out.println("Invalid integer");
            continue;
         }
      }
      try {
         PreparedStatement deleteAlbum = conn.prepareStatement(queryString);
         deleteAlbum.setInt(1, id);
         int deleted = deleteAlbum.executeUpdate();
         System.out.println("Successfully executed deletion: Deleted " + deleted + " row(s)\n");
         deleteAlbum.close();
      } catch (Exception e) {
         System.out.println("Failed to delete album.\n");
      }
   }
   public static boolean isDuplicateSong(String author, String title, Connection conn) {
      String sql = "SELECT * FROM song WHERE author = ? AND title = ?";
      try(PreparedStatement statement = conn.prepareStatement(sql) ) {
         statement.setString(1, author);
         statement.setString(2, title);
         return statement.executeQuery().isBeforeFirst();
      } catch(SQLException e) {
         e.printStackTrace();
      }
      return false;
   }

   public static void insertSong(Scanner scanner, Connection conn) {
      System.out.println("\n\nAdd a song\nRequired field *");
      String author = getInputOnAttribute(scanner, "Enter author", "author", true);
      String title = getInputOnAttribute(scanner, "Enter title", "title", true);
      if(isDuplicateSong(author, title, conn)) {
         System.out.println("A song with that author and title already exists.\n");
         return;
      }

      String albumIdString = getInputOnAttribute(scanner, "Enter album ID", "album ID", false);
      while(!albumIdString.trim().isEmpty() && !albumIdString.matches("\\d+")) {
         System.out.println("Album ID must be an integer");
         albumIdString = getInputOnAttribute(scanner, "Enter album ID", "album ID", false);
      }

      int albumId = 0;
      boolean isSingle = albumIdString.isEmpty();
      if(!isSingle) albumId = Integer.parseInt(albumIdString);

      String sql = "INSERT INTO song VALUES (?, ?"+(!isSingle ? ", ?" : "")+")";
      try(PreparedStatement statement = conn.prepareStatement(sql)) {
         statement.setString(1, author);
         statement.setString(2, title);
         if(!isSingle) statement.setInt(3, albumId);

         statement.execute();
         statement.close();
         System.out.println("Successfully inserted\n");
      } catch(SQLException e) {
         e.printStackTrace();
      }
   }

   public static boolean isValidDate(String dateString, String pattern) {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
      try {
         LocalDate.parse(dateString, formatter);
         return true; // If parsing succeeds, it's a valid date
      } catch (DateTimeParseException e) {
         return false; // Parsing failed, so it's invalid
      }
   }

   public static void insertAlbum(Scanner scanner, Connection conn) {
      System.out.println("\n\nAdd an album\nRequired field *");
      String format = getInputOnAttribute(scanner, "Enter format", "format", true);
      String title = getInputOnAttribute(scanner, "Enter title", "title", true);
      String identifier = getInputOnAttribute(scanner, "Enter identifier", "identifier", true);
      while(isDuplicateKey(identifier, "album", "identifier", conn)) {
         System.out.println("'"+identifier+"' is already in the database.\n");
         identifier = getInputOnAttribute(scanner, "Enter identifier", "identifier", true);
      }

      String copyrightDateStr =  getInputOnAttribute(scanner, "Enter copyright date (format:yyyy-mm-dd)", "copyright date", true);
      while(!isValidDate(copyrightDateStr, "yyyy-MM-dd")) {
         System.out.println("'" + copyrightDateStr + "' is not a valid date");
         copyrightDateStr = getInputOnAttribute(scanner, "Enter copyright date (format:yyyy-mm-dd)", "copyright date", true);
      }
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
      LocalDate localDate = LocalDate.parse(copyrightDateStr, formatter);
      Date copyrightDate = Date.valueOf(localDate);

      System.out.println("An album requires at least one song.");
      String author = getInputOnAttribute(scanner, "Enter song author", "author", true);
      String songTitle = getInputOnAttribute(scanner, "Enter song title", "title", true);

      try {
         String sql = "INSERT INTO Album (format, title, identifier, copyrightDate) VALUES (?, ?, ?, ?) RETURNING id";
         conn.setAutoCommit(false);
         PreparedStatement statement = conn.prepareStatement(sql);
         statement.setString(1, format);
         statement.setString(2, title);
         statement.setString(3, identifier);
         statement.setDate(4, copyrightDate);

         ResultSet row = statement.executeQuery();
         int albumID = (row.next()) ? row.getInt("id") : 0;
         System.out.println(albumID);
         sql = "SELECT * FROM Song WHERE author = ? AND title = ?";
         statement = conn.prepareStatement(sql);
         statement.setString(1, author);
         statement.setString(2, songTitle);
         if(statement.executeQuery().isBeforeFirst()) {
            sql = "UPDATE Song SET albumID = ? WHERE author = ? and title = ?";
            statement = conn.prepareStatement(sql);
            statement.setInt(1, albumID);
            statement.setString(2, author);
            statement.setString(3, songTitle);
            statement.executeUpdate();
         } else {
            sql = "INSERT INTO Song VALUES (?, ?, ?)";
            statement = conn.prepareStatement(sql);
            statement.setString(1, author);
            statement.setString(2, songTitle);
            statement.setInt(3, albumID);
            statement.execute();
         }
         conn.commit();
         conn.setAutoCommit(true);
         System.out.println("Successfully Inserted");
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
