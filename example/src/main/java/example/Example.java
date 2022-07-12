package example;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.Transact.Region.*;

public class Example {
  private static class Customer implements DeepCloneable<Customer> {
    private String firstName;

    private String lastName;

    private String email;

    private int balance;

    Customer(String firstName, String lastName, String email) {
      this.firstName = firstName;
      this.lastName = lastName;
      this.email = email;
    }

    public String getFirstName() {
      return firstName;
    }

    public void setFirstName(String firstName) {
      this.firstName = firstName;
    }

    public String getLastName() {
      return lastName;
    }

    public void setLastName(String lastName) {
      this.lastName = lastName;
    }

    public String getEmail() {
      return email;
    }

    public void setEmail(String email) {
      this.email = email;
    }

    public int getBalance() { return balance; }

    public void setBalance(int balance) { this.balance = balance; }

    @Override
    public String toString() {
      return Customer.class.getSimpleName() + "[firstName=" + firstName + ", lastName=" + lastName + ", email=" + email + ", balance=" + balance + ']';
    }

    @Override
    public Customer deepClone() {
      final var copy = new Customer(firstName, lastName, email);
      copy.balance = balance;
      return copy;
    }
  }

  public static void main(String[] args) {
    final var map = new Ss2plMap<String, Customer>(new Ss2plMap.Options());

    // Atomically insert a pair of customers.
    Transact.over(map).run(ctx -> {
      ctx.insert("john.citizen", new Customer("John", "Citizen", "john.citizen@local"));
      ctx.insert("jane.citizen", new Customer("Jane", "Citizen", "jane.citizen@local"));

      System.out.format("there are now %d customers%n", ctx.size());
      return Action.COMMIT;      // upon commitment, either both customers will exist or neither
    });

    // Atomically move funds between customers.
    Transact.over(map).run(ctx -> {
      final var customer1 = ctx.read("john.citizen");
      final var customer2 = ctx.read("jane.citizen");

      final var amountToTransfer = 100;

      customer1.setBalance(customer1.getBalance() - amountToTransfer);
      customer2.setBalance(customer2.getBalance() + amountToTransfer);
      ctx.update("john.citizen", customer1);
      ctx.update("jane.citizen", customer2);

      if (customer1.getBalance() < 0) {
        System.out.format("Rolling back due to negative funds (%d)%n", customer1.getBalance());
        return Action.ROLLBACK;  // never leave the customer with a –ve balance
      }

      return Action.COMMIT;      // upon commitment, the funds be moved
    });

    // Change a customer's attribute.
    final var completed = Transact.over(map).run(ctx -> {
      final var customer = ctx.read("jane.citizen");
      System.out.format("customer: %s%n", customer);
      customer.setEmail("jane.citizen@freemail.org");
      ctx.update("jane.citizen", customer);
      return Action.COMMIT;
    });
    System.out.format("completed version: %d%n", completed.getVersion());

    // Delete one of the customers.
    Transact.over(map).run(ctx -> {
      ctx.delete("jane.citizen");
      return Action.COMMIT;
    });

    // Print all customers whose key ends with '.citizen'.
   Transact.over(map).run(ctx -> {
      final var keys = ctx.keys(key -> key.endsWith(".citizen"));
      for (var key : keys) {
        System.out.format("key: %s, value: %s%n", key, ctx.read(key));
      }
      return Action.ROLLBACK;    // a noncommittal transaction
    });
  }
}
