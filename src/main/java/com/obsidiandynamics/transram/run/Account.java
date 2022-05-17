package com.obsidiandynamics.transram.run;

import com.obsidiandynamics.transram.*;

public final class Account implements DeepCloneable<Account> {
  private final int id;

  private int balance;

  public Account(int id, int balance) {
    this.id = id;
    this.balance = balance;
  }

  public int getId() {
    return id;
  }

  public int getBalance() {
    return balance;
  }

  public void setBalance(int balance) {
    this.balance = balance;
  }

  @Override
  public String toString() {
    return Account.class.getSimpleName() + "[" +
        "id=" + id +
        ", amount=" + balance +
        ']';
  }

  @Override
  public Account deepClone() {
    return new Account(id, balance);
  }
}
