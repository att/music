package com.att.research.music.datastore.jsonobjects;

public class JsonLeasedLock {
	long leasePeriod;
	String notifyUrl;
	

	public long getLeasePeriod() {
		return leasePeriod;
	}
	public void setLeasePeriod(long leasePeriod) {
		this.leasePeriod = leasePeriod;
	}
	public String getNotifyUrl() {
		return notifyUrl;
	}
	public void setNotifyUrl(String notifyUrl) {
		this.notifyUrl = notifyUrl;
	}
}
