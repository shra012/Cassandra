package com.cassandra.driver.dao;

import java.time.LocalDate;
import java.util.UUID;

import com.cassandra.driver.VideosByTag;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Select;

@Dao
public interface VideosByTagDao {

	@Select
	PagingIterable<VideosByTag> all();

	@Select
	PagingIterable<VideosByTag> findByTag(String tag);
	
	@Select
	VideosByTag findByTagAddedDateAndVideoId(String tag,LocalDate addedDate,UUID videoId);

	@Insert
	void save(VideosByTag videosByTag);

	@Delete
	void delete(VideosByTag videosByTag);
}
