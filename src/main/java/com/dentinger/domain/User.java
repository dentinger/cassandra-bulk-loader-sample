package com.dentinger.domain;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class User {
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private String username;
	private String email;
	private String description;
	private String password;
	private Date createdDate;
	private Date modified_date;
	private String firstname;
	private String lastname;

	public Date getCreatedDate() {
		return this.createdDate;
	}

	public String getDescription() {
		return this.description;
	}

	public String getEmail() {
		return this.email;
	}

	public String getFirstname() {
		return this.firstname;
	}

	public String getLastname() {
		return this.lastname;
	}

	public Date getModified_date() {
		return this.modified_date;
	}

	public String getPassword() {
		return this.password;
	}

	public String getUsername() {
		return this.username;
	}

	public void setCreatedDate(final Date createdDate) {
		this.createdDate = createdDate;
	}

	public void setDescription(final String description) {
		this.description = description;
	}

	public void setEmail(final String email) {
		this.email = email;
	}

	public void setFirstname(final String firstname) {
		this.firstname = firstname;
	}

	public void setLastname(final String lastname) {
		this.lastname = lastname;
	}

	public void setModified_date(final Date modified_date) {
		this.modified_date = modified_date;
	}

	public void setPassword(final String password) {
		this.password = password;
	}

	public void setUsername(final String username) {
		this.username = username;
	}

  public String toCSVString() {
    String firstPart = username + "," + email + "," + description + "," + password + "," + firstname
        + "," + lastname + ",";
    if(createdDate!= null) {
      firstPart += firstPart + dateFormat.format(createdDate) ;
    }
    firstPart +=  ",";
    if(modified_date != null) {
      firstPart += firstPart +  dateFormat.format(modified_date);
    }
    return firstPart ;
  }
}
