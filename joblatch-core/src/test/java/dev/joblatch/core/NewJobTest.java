package dev.joblatch.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/** Unit tests: no database needed. They check that invalid jobs cannot even be created. */
class NewJobTest {

    private static final String VALID_JSON = "{\"orderId\":1}";

    @Test
    void validJobGetsDefaults() {
        NewJob job = NewJob.of("emails", "send-receipt", VALID_JSON);

        assertThat(job.runAt()).isNull(); // null = "now" on the database clock
        assertThat(job.priority()).isEqualTo(NewJob.DEFAULT_PRIORITY);
        assertThat(job.maxAttempts()).isEqualTo(NewJob.DEFAULT_MAX_ATTEMPTS);
    }

    // @ParameterizedTest runs the same test once per value listed.
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "has space", "semi;colon", "quote'"})
    void rejectsBadQueueNames(String queue) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> NewJob.of(queue, "send-receipt", VALID_JSON));
    }

    @Test
    void rejectsQueueNameLongerThan128() {
        String tooLong = "q".repeat(129);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> NewJob.of(tooLong, "send-receipt", VALID_JSON));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"  "})
    void rejectsMissingPayload(String payload) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> NewJob.of("emails", "send-receipt", payload));
    }

    @Test
    void rejectsOversizedPayloadWithoutEchoingIt() {
        // Build a JSON string just over the limit.
        String big = "\"" + "x".repeat(NewJob.MAX_PAYLOAD_BYTES) + "\"";

        assertThatIllegalArgumentException()
                .isThrownBy(() -> NewJob.of("emails", "send-receipt", big))
                .withMessageNotContaining("xxxx"); // reports the size, not the content
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void rejectsMaxAttemptsBelowOne(int maxAttempts) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> NewJob.of("emails", "send-receipt", VALID_JSON)
                        .withMaxAttempts(maxAttempts));
    }

    @Test
    void toStringNeverContainsPayload() {
        NewJob job = NewJob.of("emails", "send-receipt", "{\"ssn\":\"SECRET-123\"}");

        assertThat(job.toString())
                .doesNotContain("SECRET-123")
                .contains("payload=<redacted>");
    }
}