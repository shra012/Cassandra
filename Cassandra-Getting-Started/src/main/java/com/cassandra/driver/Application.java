package com.cassandra.driver;

import java.time.LocalDate;

import com.cassandra.driver.codecs.LocalDateCodec;
import com.cassandra.driver.dao.VideosByTagDao;
import com.cassandra.driver.mapper.VideosMapper;
import com.cassandra.driver.mapper.VideosMapperBuilder;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.uuid.Uuids;

/**
 * Hello world!
 *
 */
public class Application 
{
	public static void main( String[] args )
	{
		try (CqlSession session = CqlSession.builder().addTypeCodecs(new LocalDateCodec()).withKeyspace("killrvideo").build()) {
			VideosMapper mapper = new VideosMapperBuilder(session).build();
			VideosByTagDao dao = mapper.videoDao();
			VideosByTag videosByTag = new VideosByTag(Uuids.timeBased(),"datastax","DataStax Academy",LocalDate.now());
			dao.save(videosByTag);
			
			VideosByTag readVideosByTag = dao.findByTagAddedDateAndVideoId(videosByTag.getTag(), videosByTag.getAddedDate(), videosByTag.getVideoId());

			System.out.println(readVideosByTag);
			
			dao.delete(videosByTag);
		}
	}
}
