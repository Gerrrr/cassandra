/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools.nodetool;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import io.airlift.command.Command;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "listendpointspendinghints", description = "Print all the endpoints that this node has hints for")
public class ListEndpointsPendingHints extends NodeTool.NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        Map<String, Map<String, String>> endpoints = probe.listEndpointsPendingHints();
        if(endpoints.isEmpty())
        {
            System.out.println("This node does not have hints for other endpoints");
        }
        else
        {
            Map<String, String> endpointMap = probe.getEndpointMap();
            Map<String, String> simpleStates = probe.getFailureDetectorSimpleStates();
            EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            TableBuilder tb = new TableBuilder();

            tb.add("Host ID", "Address", "Rack", "DC", "Status", "Total files", "Newest", "Oldest");
            for (Map.Entry<String, Map<String, String>> entry : endpoints.entrySet())
            {
                String endpoint = entry.getKey();
                String address = endpointMap.get(endpoint);
                String rack;
                String dc;
                String status;
                try
                {
                    rack = epSnitchInfo.getRack(address);
                    dc = epSnitchInfo.getDatacenter(address);
                    status = simpleStates.getOrDefault(InetAddress.getByName(address).toString(), "Unknown");
                }
                catch (UnknownHostException e)
                {
                    rack = "Unknown";
                    dc = "Unknown";
                    status = "Unknown";
                }
                tb.add(
                    endpoint,
                    address,
                    rack,
                    dc,
                    status,
                    entry.getValue().get("totalFiles"),
                    sdf.format(new Date(Long.parseLong(entry.getValue().get("newest")))),
                    sdf.format((new Date(Long.parseLong(entry.getValue().get("oldest")))))
                );
            }
            tb.printTo(System.out);
        }
    }
}
