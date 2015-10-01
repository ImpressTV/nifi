/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.QueueSize;

public interface FlowFileQueue {

    /**
     * @return the unique identifier for this FlowFileQueue
     */
    String getIdentifier();

    /**
     * @return list of processing priorities for this queue
     */
    List<FlowFilePrioritizer> getPriorities();

    /**
     * @return the minimum number of FlowFiles that must be present in order for
     * FlowFiles to begin being swapped out of the queue
     */
    int getSwapThreshold();

    /**
     * Resets the comparator used by this queue to maintain order.
     *
     * @param newPriorities the ordered list of prioritizers to use to determine
     * order within this queue.
     * @throws NullPointerException if arg is null
     */
    void setPriorities(List<FlowFilePrioritizer> newPriorities);

    /**
     * Establishes this queue's preferred maximum work load.
     *
     * @param maxQueueSize the maximum number of flow files this processor
     * recommends having in its work queue at any one time
     */
    void setBackPressureObjectThreshold(long maxQueueSize);

    /**
     * @return maximum number of flow files that should be queued up at any one
     * time
     */
    long getBackPressureObjectThreshold();

    /**
     * @param maxDataSize Establishes this queue's preferred maximum data size.
     */
    void setBackPressureDataSizeThreshold(String maxDataSize);

    /**
     * @return maximum data size that should be queued up at any one time
     */
    String getBackPressureDataSizeThreshold();

    QueueSize size();

    /**
     * @return total size in bytes of the queue flow file's content
     */
    long contentSize();

    /**
     * @return true if no items queue; false otherwise
     */
    boolean isEmpty();

    /**
     * @return true if the active queue is empty; false otherwise. The Active
     * queue contains those FlowFiles that can be processed immediately and does
     * not include those FlowFiles that have been swapped out or are currently
     * being processed
     */
    boolean isActiveQueueEmpty();

    QueueSize getActiveQueueSize();

    /**
     * Removes all of the items from this queue.
     *
     */
    void clear();

    /**
     * Removes a single item from this queue, if it is present. Returns true if and only if this
     * queue contained the specified item.
     *
     * @param record Item to be removed
     * @reutrn true if this queue changed as a result of the call
     */
    boolean remove(FlowFileRecord record);

    /**
     * Returns a QueueSize that represents all FlowFiles that are 'unacknowledged'. A FlowFile
     * is considered to be unacknowledged if it has been pulled from the queue by some component
     * but the session that pulled the FlowFile has not yet been committed or rolled back.
     *
     * @return a QueueSize that represents all FlowFiles that are 'unacknowledged'.
     */
    QueueSize getUnacknowledgedQueueSize();

    void acknowledge(FlowFileRecord flowFile);

    void acknowledge(Collection<FlowFileRecord> flowFiles);

    /**
     * @return true if maximum queue size has been reached or exceeded; false
     * otherwise
     */
    boolean isFull();

    /**
     * places the given file into the queue
     *
     * @param file to place into queue
     */
    void put(FlowFileRecord file);

    /**
     * places the given files into the queue
     *
     * @param files to place into queue
     */
    void putAll(Collection<FlowFileRecord> files);

    /**
     * Removes all records from the internal swap queue and returns them.
     *
     * @return all removed records from internal swap queue
     */
    List<FlowFileRecord> pollSwappableRecords();

    /**
     * Restores the records from swap space into this queue, adding the records
     * that have expired to the given set instead of enqueuing them.
     *
     * @param records that were swapped in
     */
    void putSwappedRecords(Collection<FlowFileRecord> records);

    /**
     * Updates the internal counters of how much data is queued, based on
     * swapped data that is being restored.
     *
     * @param numRecords count of records swapped in
     * @param contentSize total size of records being swapped in
     */
    void incrementSwapCount(int numRecords, long contentSize);

    /**
     * @return the number of FlowFiles that are enqueued and not swapped
     */
    int unswappedSize();

    int getSwapRecordCount();

    int getSwapQueueSize();

    /**
     * @param expiredRecords expired records
     * @return the next flow file on the queue; null if empty
     */
    FlowFileRecord poll(Set<FlowFileRecord> expiredRecords);

    /**
     * @param maxResults limits how many results can be polled
     * @param expiredRecords for expired records
     * @return the next flow files on the queue up to the max results; null if
     * empty
     */
    List<FlowFileRecord> poll(int maxResults, Set<FlowFileRecord> expiredRecords);

    /**
     * Drains flow files from the given source queue into the given destination
     * list.
     *
     * @param sourceQueue queue to drain from
     * @param destination Collection to drain to
     * @param maxResults max number to drain
     * @param expiredRecords for expired records
     * @return size (bytes) of flow files drained from queue
     */
    long drainQueue(Queue<FlowFileRecord> sourceQueue, List<FlowFileRecord> destination, int maxResults, Set<FlowFileRecord> expiredRecords);

    List<FlowFileRecord> poll(FlowFileFilter filter, Set<FlowFileRecord> expiredRecords);

    String getFlowFileExpiration();

    int getFlowFileExpiration(TimeUnit timeUnit);

    void setFlowFileExpiration(String flowExpirationPeriod);

    /**
     * Returns an ordered list of all the items in this queue.
     *
     * @return ordered list of queue items
     */
    List<FlowFileRecord> getItems();

}
