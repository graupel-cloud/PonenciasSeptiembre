package com.demo.retail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** Additive sales measures for one store x product. */
public class SalesAgg implements Writable {
  long units;
  long returnedUnits;
  long lines;
  /** tickets whose first line is this product: summing it per store gives the store's tickets */
  long tickets;
  double gross;
  double net;

  void clear() {
    units = returnedUnits = lines = tickets = 0;
    gross = net = 0;
  }

  void add(SalesAgg o) {
    units += o.units;
    returnedUnits += o.returnedUnits;
    lines += o.lines;
    tickets += o.tickets;
    gross += o.gross;
    net += o.net;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(units);
    out.writeLong(returnedUnits);
    out.writeLong(lines);
    out.writeLong(tickets);
    out.writeDouble(gross);
    out.writeDouble(net);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    units = in.readLong();
    returnedUnits = in.readLong();
    lines = in.readLong();
    tickets = in.readLong();
    gross = in.readDouble();
    net = in.readDouble();
  }
}
