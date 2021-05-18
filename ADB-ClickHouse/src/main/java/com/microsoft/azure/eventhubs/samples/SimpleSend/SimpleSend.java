/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.samples.SimpleSend;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

public class SimpleSend {

    public static void main(String[] args)
            throws EventHubException, ExecutionException, InterruptedException, IOException {
    	
    	if(args.length == 0 || args.length < 4) {
    		System.out.println("usage SimpleSend eh-namespace-name eh-name SasKeyName SasKey");
    		System.exit(0);
    	}
        final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
                .setNamespaceName(args[0]) // "Your Event Hubs namespace name" to target National clouds - use .setEndpoint(URI)
                .setEventHubName(args[1])//Your event hub
                .setSasKeyName(args[2])
                .setSasKey(args[3]);//Your primary SAS key

        final Gson gson = new GsonBuilder().create();

        // The Executor handles all asynchronous tasks and this is passed to the EventHubClient instance.
        // This enables the user to segregate their thread pool based on the work load.
        // This pool can then be shared across multiple EventHubClient instances.
        // The following sample uses a single thread executor, as there is only one EventHubClient instance,
        // handling different flavors of ingestion to Event Hubs here.
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

        // Each EventHubClient instance spins up a new TCP/SSL connection, which is expensive.
        // It is always a best practice to reuse these instances. The following sample shows this.
        final EventHubClient ehClient = EventHubClient.createSync(connStr.toString(), executorService);
        FileInputStream fis = null;
        File file = null;
        
        try {
        	byte[] bytesArray = new byte[1024];
        	
        	File dir = new File("../data");
        	File[] files = dir.listFiles();
        	for (int i = 1; i < files.length; i++) {

                file = new File(files[i].getAbsolutePath());
                fis = new FileInputStream(file);
                fis.read(bytesArray);
                EventData sendEvent = EventData.create(bytesArray);

                // Send - not tied to any partition
                // Event Hubs service will round-robin the events across all Event Hubs partitions.
                // This is the recommended & most reliable way to send to Event Hubs.
                ehClient.sendSync(sendEvent);
                System.out.println("File Sent -->"+file.getName());
                System.out.println("Waiting.....");
                Thread.currentThread().sleep(500);
                System.out.println("Resuming.....");
            }

            System.out.println(Instant.now() + ": Send Complete...");
            System.out.println("Press Enter to stop.");
            System.in.read();
        } finally {
            ehClient.closeSync();
            executorService.shutdown();
            fis.close();
        }
    }
}
