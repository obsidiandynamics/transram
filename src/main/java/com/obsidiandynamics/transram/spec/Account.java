package com.obsidiandynamics.transram.spec;

import com.obsidiandynamics.transram.*;

final class Account implements DeepCloneable<Account> {
  private final int id;

  private int balance;

  Account(int id, int balance) {
    this.id = id;
    this.balance = balance;
  }

  int getId() {
    return id;
  }

  int getBalance() {
    return balance;
  }

  void setBalance(int balance) {
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
