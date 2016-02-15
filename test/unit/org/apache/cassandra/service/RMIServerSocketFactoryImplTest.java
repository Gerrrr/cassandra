package org.apache.cassandra.service;

import java.io.IOException;
import java.net.ServerSocket;
import java.rmi.server.RMIServerSocketFactory;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

import org.apache.cassandra.utils.RMIServerSocketFactoryImpl;


public class RMIServerSocketFactoryImplTest
{
    @Test
    public void testReusableAddrSocket() throws IOException
    {
        RMIServerSocketFactory serverFactory = new RMIServerSocketFactoryImpl();
        ServerSocket socket = serverFactory.createServerSocket(7199);
        assertTrue(socket.getReuseAddress());
    }

}
