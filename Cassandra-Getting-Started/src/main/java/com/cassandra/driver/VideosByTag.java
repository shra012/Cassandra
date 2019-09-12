package com.cassandra.driver;

import java.time.LocalDate;
import java.util.UUID;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

@Entity(defaultKeyspace = "killrvideo")
@CqlName("videos_by_tag")
public class VideosByTag {
	
	@ClusteringColumn(2)
	@CqlName("video_id")
	private UUID videoId;
	@PartitionKey
	@CqlName("tag")
	private String tag;
	@CqlName("title")
	private String title;
	@ClusteringColumn(1)
	@CqlName("added_date")
	private LocalDate addedDate;
	
	public VideosByTag() {
	}
	
	public VideosByTag(UUID videoId, String tag, String title, LocalDate addedDate) {
		this.videoId = videoId;
		this.tag = tag;
		this.title = title;
		this.addedDate = addedDate;
	}
	
	public UUID getVideoId() {
		return videoId;
	}
	public void setVideoId(UUID videoId) {
		this.videoId = videoId;
	}
	public String getTag() {
		return tag;
	}
	public void setTag(String tag) {
		this.tag = tag;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}

	public LocalDate getAddedDate() {
		return addedDate;
	}

	public void setAddedDate(LocalDate addedDate) {
		this.addedDate = addedDate;
	}

	@Override
	public String toString() {
		return "VideosByTag [videoId=" + videoId + ", tag=" + tag + ", title=" + title + ", addedDate=" + addedDate
				+ "]";
	}
	
	
}
