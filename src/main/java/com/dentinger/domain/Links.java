package com.dentinger.domain;

import java.util.HashSet;
import java.util.Set;


public class Links {
  private String id;
  private Set<String> ins;
  private Set<String> outs;

  public Set<String> getOuts() {
    if(outs == null) outs = new HashSet<String>();
    return outs;
  }

  public void setOuts(Set<String> outs) {
    this.outs = outs;
  }

  public Set<String> getIns() {
    if(ins == null) ins = new HashSet<String>();
    return ins;
  }

  public void setIns(Set<String> ins) {
    this.ins = ins;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }



}
