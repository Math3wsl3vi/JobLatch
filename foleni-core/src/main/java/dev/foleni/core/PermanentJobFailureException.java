package dev.foleni.core;

/**
 * Thrown by a {@link JobHandler} when retrying cannot possibly help: the payload is invalid, the
 * customer no longer exists, the request is forbidden. The job goes straight to 'dead' instead of
 * using up its remaining attempts.
 *
 * <p>It extends {@link RuntimeException} (an "unchecked" exception), so handlers can throw it from
 * anywhere without declaring it.
 */
public class PermanentJobFailureException extends RuntimeException {

    // Exceptions are Serializable in Java. This number marks the version of the class's
    // serialized form. -Xlint:all warns if it is missing.
    private static final long serialVersionUID = 1L;

    /** @param message why the job can never succeed. Stored for operators. No secrets. */
    public PermanentJobFailureException(String message) {
        super(message);
    }

    /** Same, keeping the original exception as the cause. */
    public PermanentJobFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
