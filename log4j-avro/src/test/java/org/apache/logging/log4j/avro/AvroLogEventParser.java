package org.apache.logging.log4j.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.AvroRuntimeException;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.parser.LogEventParser;
import org.apache.logging.log4j.core.parser.ParseException;

/**
 * Parses the output from {@link AvroLayout} into instances of {@link LogEvent}.
 */
public class AvroLogEventParser implements LogEventParser {

  @Override
  public LogEvent parseFrom(final byte[] input) throws ParseException {
    org.apache.logging.log4j.avro.LogEvent datum;
    try {
      datum = org.apache.logging.log4j.avro.LogEvent.getDecoder().decode(input);
    } catch (IOException | AvroRuntimeException e) {
      throw new ParseException(e);
    }

    return parseFrom(datum);
  }

  @Override
  public LogEvent parseFrom(final byte[] input, final int offset, final int length)
      throws ParseException {
    org.apache.logging.log4j.avro.LogEvent datum;
    try {
      ByteBuffer byteBuffer = ByteBuffer.wrap(input, offset, length);
      datum = org.apache.logging.log4j.avro.LogEvent.getDecoder().decode(byteBuffer);
    } catch (IOException | AvroRuntimeException e) {
      throw new ParseException(e);
    }

    return parseFrom(datum);
  }

  private LogEvent parseFrom(final org.apache.logging.log4j.avro.LogEvent datum) {
    Log4jLogEvent.Builder builder = Log4jLogEvent.newBuilder();
    // TODO
    return builder.build();
  }

}
