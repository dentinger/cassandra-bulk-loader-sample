package com.dentinger.sample.util;

import com.dentinger.domain.User;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Created by dan on 8/26/14.
 */
public class NamesGenerator {
  Set<String> firsts;
  Set<String> lasts;
  Set<String> fulls;

  Random random;
  public NamesGenerator(List<String> all) {
    random = new Random(System.currentTimeMillis());
    firsts = new HashSet<String>();
    lasts = new HashSet<String>();
    fulls = new HashSet<String>();
    //first go thourgh and each of the full names from all to fulls,
    //then split it and add to first last
    List<Map<String, String>> names = Lists.transform(all, new Function<String, Map<String, String>>() {
      @Override public Map<String,String> apply(String input) {
        Map<String, String> ret = new HashMap<String, String>();
          ret.put("full", input);
          String[] both = input.split("\\s");
        ret.put("first", both[0]);
        ret.put("last", both[1]);
        return ret;
      }
    });

    for(Map<String, String> tuple: names) {
      firsts.add(tuple.get("first"));
      lasts.add(tuple.get("last"));
      fulls.add(tuple.get("full"));
    }
  }

  public String[] getNextName() {
    String[] names= new String[2];
    names[0]=(String)firsts.toArray()[random.nextInt(firsts.size())];

    names[1]= (String)lasts.toArray()[random.nextInt(lasts.size())];

    return names;
  }
}
