package dev.joblatch.core;

import java.util.regex.Pattern;

/**
 * The one place that defines what a valid queue name, job type or worker ID looks like.
 *
 * <p>The same rule appears as {@code CHECK} constraints in {@code V1__create_jobs.sql}. If you
 * change one, change the other.
 *
 * <p>Package-private (no {@code public}): only classes in {@code dev.joblatch.core} can use it, so it
 * is not part of the library's public API.
 */
final class Names {

    private static final Pattern PATTERN = Pattern.compile("[A-Za-z0-9_.:-]{1,128}");

    private Names() {}

    /**
     * Throws {@link IllegalArgumentException} unless {@code value} is 1-128 characters of
     * {@code A-Z a-z 0-9 _ . : -}. The message never repeats the value, because an invalid value
     * could be anything, including sensitive data passed in by mistake.
     */
    static void requireValid(String field, String value) {
        if (value == null || !PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " must be 1-128 characters of A-Z a-z 0-9 _ . : -");
        }
    }
}