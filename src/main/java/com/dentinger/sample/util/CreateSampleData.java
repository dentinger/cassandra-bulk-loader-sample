package com.dentinger.sample.util;

import com.dentinger.domain.User;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;

/**
 * Created by dan on 8/26/14.
 */
public class CreateSampleData {
  private BufferedWriter out;
  private int totalTrans = 50000;
  private List<User> users;
  public CreateSampleData() throws IOException {
    out = new BufferedWriter(new FileWriter("src/main/resources/Transactions.csv"));

    int batch = 1000;
    int cycles = this.totalTrans / batch;

    for (int i=0; i < cycles; i++){
     users  = generateUsers(batch);
      this.writeToFile(users);

      if (cycles % 1000 == 0){
        System.out.println("Wrote " + i + " of " + cycles + " cycles.");
      }
    }

    out.close();
    System.out.println("Finished file with " + this.totalTrans + " transactions.");
   // System.exit(0);
  }
  public List<User> getList() {
    return users;
  }
  public List<User> generateUsers(int batchSize) throws IOException {
    List<User> users = new ArrayList<User>(batchSize);
    List<String> names = FileUtils.readLines(new File("src/main/resources/names.txt") );
    NamesGenerator namesGenerator = new NamesGenerator(names);
    final String pw = "12345";
    for(int i = 0; i < batchSize; i++) {
      User u = new User();
      String[] name = namesGenerator.getNextName();
      u.setFirstname(name[0]);
      u.setLastname(name[1]);
      u.setUsername(name[0]+name[1]);
      u.setPassword(pw);
      u.setEmail(name[0]+"@"+name[1]);
      u.setDescription("Hi this is " + u.getUsername());
      users.add(u);
    }

  return users;
  }
  public void writeToFile(List<User> users) throws IOException {
    try {
      for (User user : users) {
        out.write(user.toCSVString() + "\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }

  public static void main(String[] args) {
    try {
      new CreateSampleData();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
