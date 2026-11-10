/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderValues.SNAPPY;
import static io.netty.handler.codec.http2.Http2Error.FLOW_CONTROL_ERROR;
import static io.netty.handler.codec.http2.Http2Exception.streamError;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DelegatingDecompressorFrameListenerTest {
    private static final int STREAM_ID = 3;

    private EmbeddedChannel channel;
    private ChannelHandlerContext ctx;
    private Http2Connection connection;
    private Http2LocalFlowController flowController;
    private Http2FrameListener listener;
    private DelegatingDecompressorFrameListener decompressorListener;

    @BeforeEach
    public void setUp() throws Http2Exception {
        channel = new EmbeddedChannel();
        ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);

        connection = new DefaultHttp2Connection(true);
        flowController = mock(Http2LocalFlowController.class);
        connection.local().flowController(flowController);
        connection.remote().createStream(STREAM_ID, false);

        listener = mock(Http2FrameListener.class);
        decompressorListener = new DelegatingDecompressorFrameListener(connection, listener);
        decompressorListener.onHeadersRead(ctx, STREAM_ID,
                new DefaultHttp2Headers().set(CONTENT_ENCODING, SNAPPY), 0, false);
    }

    @AfterEach
    public void tearDown() {
        Http2Stream stream = connection.stream(STREAM_ID);
        if (stream != null) {
            stream.close();
        }
        channel.finishAndReleaseAll();
    }

    @Test
    public void testDecompressedBuffersReleasedOnSuccess() throws Exception {
        ByteBuf data = snappyFramed("abc", "def");
        try {
            assertEquals(0, decompressorListener.onDataRead(ctx, STREAM_ID, data, 0, true));
            verify(listener).onDataRead(eq(ctx), eq(STREAM_ID), any(ByteBuf.class), eq(0), eq(false));
            verify(listener).onDataRead(eq(ctx), eq(STREAM_ID), any(ByteBuf.class), eq(0), eq(true));
            assertEquals(1, data.refCnt());
        } finally {
            data.release();
        }
    }

    @Test
    public void testDecompressedBuffersReleasedWhenListenerThrows() throws Exception {
        when(listener.onDataRead(any(ChannelHandlerContext.class), anyInt(), any(ByteBuf.class), anyInt(),
                anyBoolean())).thenThrow(new IllegalStateException("listener failure"));
        assertDecompressedBuffersReleasedOnFailure();
    }

    @Test
    public void testDecompressedBuffersReleasedWhenFlowControllerThrows() throws Exception {
        when(flowController.consumeBytes(any(Http2Stream.class), anyInt()))
                .thenThrow(streamError(STREAM_ID, FLOW_CONTROL_ERROR, "flow control failure"));
        assertDecompressedBuffersReleasedOnFailure();
    }

    private void assertDecompressedBuffersReleasedOnFailure() {
        // A single DATA frame carrying two snappy chunks is decompressed into two buffers, so the second
        // (look-ahead) buffer is already held when the first one is passed to the listener.
        final ByteBuf data = snappyFramed("abc", "def");
        try {
            assertThrows(Http2Exception.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    decompressorListener.onDataRead(ctx, STREAM_ID, data, 0, false);
                }
            });
            // Uncompressed snappy chunks are retained slices of the input, so every decompressed buffer that was
            // not released keeps an extra reference on it.
            assertEquals(1, data.refCnt());
        } finally {
            data.release();
        }
    }

    private static ByteBuf snappyFramed(String... chunks) {
        ByteBuf framed = Unpooled.buffer();
        // Stream identifier chunk.
        framed.writeByte(0xff).writeMediumLE(6).writeBytes("sNaPpY".getBytes(CharsetUtil.US_ASCII));
        for (String chunk : chunks) {
            byte[] bytes = chunk.getBytes(CharsetUtil.US_ASCII);
            // Uncompressed data chunk: type, length (checksum + data), checksum (not validated) and data.
            framed.writeByte(0x01).writeMediumLE(4 + bytes.length).writeIntLE(0).writeBytes(bytes);
        }
        return framed;
    }
}
