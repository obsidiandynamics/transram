package com.obsidiandynamics.transram.util;

public final class Table {
  public static String layout(int[] padding) {
    final var sb = new StringBuilder();
    for (var j : padding) {
      sb.append('%').append(j).append('s').append('|');
    }
    return sb.append('\n').toString();
  }

  public static Object[] fill(int[] padding, char ch) {
    final var fields = new String[padding.length];
    for (var i = 0; i < padding.length; i++) {
      fields[i] = String.valueOf(ch).repeat(padding[i]);
    }
    return fields;
  }
}
