package org.apache.cassandra.transport;

import java.net.InetAddress;

import org.junit.Test;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertTrue;


public class ServerTest
{
    @Test
    public void testReusableSocket()
    {
        int nativePort = DatabaseDescriptor.getNativeTransportPort();
        InetAddress nativeAddr = DatabaseDescriptor.getRpcAddress();

        Server.Builder builder = new Server.Builder().withHost(nativeAddr).withPort(nativePort);
        Server server = builder.build();
        server.start();
        assertTrue(server.isRunning());
        ChannelGroup channels = server.getChannels();
        assertTrue(channels.size() > 0);
        channels.forEach((Channel ch) -> assertTrue(ch.config().getOption(ChannelOption.SO_REUSEADDR)));
    }
}
