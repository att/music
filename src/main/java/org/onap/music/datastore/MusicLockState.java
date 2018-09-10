package org.onap.music.datastore;
public class MusicLockState {
    public enum LockStatus {
        UNLOCKED, BEING_LOCKED, LOCKED
    };// captures the state of the lock
    
    private LockStatus lockStatus;
    private String lockHolder;
    private String errorMessage = null;
    
    public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public MusicLockState(LockStatus lockStatus, String lockHolder) {
		super();
        this.lockStatus = lockStatus;
		this.lockHolder = lockHolder;
	}

	public LockStatus getLockStatus() {
        return lockStatus;
    }

    public void setLockStatus(LockStatus lockStatus) {
        this.lockStatus = lockStatus;
    }

    public String getLockHolder() {
        return lockHolder;
    }

    public void setLockHolder(String lockHolder) {
        this.lockHolder = lockHolder;
    }

}
