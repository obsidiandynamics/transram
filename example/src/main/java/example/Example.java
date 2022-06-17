package example;

import com.obsidiandynamics.transram.*;
import com.obsidiandynamics.transram.SrmlMap.*;
import com.obsidiandynamics.transram.Transact.Region.*;

public class Example {
  private static class Customer implements DeepCloneable<Customer> {
    private String firstName;

    private String lastName;

    private String email;

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

    @Override
    public String toString() {
      return Customer.class.getSimpleName() + "[firstName=" + firstName + ", lastName=" + lastName + ", email=" + email + ']';
    }

    @Override
    public Customer deepClone() {
      return new Customer(firstName, lastName, email);
    }
  }

  public static void main(String[] args) {
    final var map = new SrmlMap<String, Customer>(new Options());

    // Atomically insert a pair of customers.
    Transact.over(map).run(ctx -> {
      ctx.insert("john.citizen", new Customer("John", "Citizen", "john.citizen@local"));
      ctx.insert("jane.citizen", new Customer("Jane", "Citizen", "jane.citizen@local"));

      System.out.format("there are now %d customers%n", ctx.size());
      return Action.COMMIT; // upon commitment, either both customers will exist or neither
    });

    // Change a customer's attribute.
    Transact.over(map).run(ctx -> {
      final var customer = ctx.read("jane.citizen");
      System.out.format("customer: %s%n", customer);
      customer.setEmail("jane.citizen@freemail.org");
      ctx.update("jane.citizen", customer);
      return Action.COMMIT;
    });

    // Print all customers whose key ends with '.citizen'.
    Transact.over(map).run(ctx -> {
      for (var key : ctx.keys(key -> key.endsWith(".citizen"))) {
        System.out.format("key: %s, value: %s%n", key, ctx.read(key));
      }
      return Action.ROLLBACK;
    });
  }
}
