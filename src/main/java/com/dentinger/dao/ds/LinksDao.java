package com.dentinger.dao.ds;

import com.dentinger.domain.Links;

/**
 * Created by dan on 8/20/14.
 */
public interface LinksDao {
  public void updateLink(final Links links);
  public Links getLinks(final String id);
  public void addInlink(String id, String inLink);
  public void addOutlink(String id, String inLink);
}
