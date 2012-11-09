package edu.osu.cse.hpcs.tableplacement.exception;

public class TablePropertyException extends Exception {

  private static final long serialVersionUID = -1626989960510305549L;

  public TablePropertyException() {
    super();
  }

  public TablePropertyException(String message) {
    super(message);
  }

  public TablePropertyException(String message, Throwable cause) {
    super(message, cause);
  }

  public TablePropertyException(Throwable cause) {
    super(cause);
  }
}
