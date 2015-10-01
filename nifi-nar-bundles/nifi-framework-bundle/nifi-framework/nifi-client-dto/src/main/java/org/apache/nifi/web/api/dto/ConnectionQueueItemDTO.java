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
package org.apache.nifi.web.api.dto;

import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * A connection queue item.
 */
@XmlType(name = "connectionQueueItem")
public class ConnectionQueueItemDTO extends NiFiComponentDTO {

    private long flowFileId;
    private String flowFileUuid;
    private String fileSize;
    private Long fileSizeBytes;
    private String contentClaimSection;
    private String contentClaimContainer;
    private String contentClaimIdentifier;
    private Long contentClaimOffset;
    private Integer priority;

    /**
     * @return Id of the FlowFile
     */
    @ApiModelProperty(
            value = "The id of the flowfile."
    )
    public Long getFlowFileId() {
        return flowFileId;
    }

    public void setFlowFileId(Long flowFileId) {
        this.flowFileId = flowFileId;
    }


    /**
     * @return UUID of the FlowFile
     */
    @ApiModelProperty(
            value = "The uuid of the flowfile."
    )
    public String getFlowFileUuid() {
        return flowFileUuid;
    }

    public void setFlowFileUuid(String flowFileUuid) {
        this.flowFileUuid = flowFileUuid;
    }

    /**
     * @return size of the FlowFile
     */
    @ApiModelProperty(
            value = "The size of the flowfile."
    )
    public String getFileSize() {
        return fileSize;
    }

    public void setFileSize(String fileSize) {
        this.fileSize = fileSize;
    }

    /**
     * @return size of the FlowFile in bytes
     */
    @ApiModelProperty(
            value = "The size of the flowfile in bytes."
    )
    public Long getFileSizeBytes() {
        return fileSizeBytes;
    }

    public void setFileSizeBytes(Long fileSizeBytes) {
        this.fileSizeBytes = fileSizeBytes;
    }

    /**
     * @return the Section in which the Content Claim lives, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The section in which the content claim lives."
    )
    public String getcontentClaimSection() {
        return contentClaimSection;
    }

    public void setContentClaimSection(String contentClaimSection) {
        this.contentClaimSection = contentClaimSection;
    }

    /**
     * @return the Container in which the Content Claim lives, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The container in which the claim lives."
    )
    public String getcontentClaimContainer() {
        return contentClaimContainer;
    }

    public void setContentClaimContainer(String contentClaimContainer) {
        this.contentClaimContainer = contentClaimContainer;
    }

    /**
     * @return the Identifier of the Content Claim, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The identifier of the content claim."
    )
    public String getcontentClaimIdentifier() {
        return contentClaimIdentifier;
    }

    public void setContentClaimIdentifier(String contentClaimIdentifier) {
        this.contentClaimIdentifier = contentClaimIdentifier;
    }

    /**
     * @return the offset into the the Content Claim where the FlowFile's content begins, or <code>null</code> if no Content Claim exists
     */
    @ApiModelProperty(
            value = "The offset into the content claim where the flowfiles content begins."
    )
    public Long getcontentClaimOffset() {
        return contentClaimOffset;
    }

    public void setContentClaimOffset(Long contentClaimOffset) {
        this.contentClaimOffset = contentClaimOffset;
    }

    /**
     * @return the priority of the queue item
     */
    @ApiModelProperty(
            value = "The priority of the queue item."
    )
    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }
}
