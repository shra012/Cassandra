package com.cassandra.driver.mapper;

import com.cassandra.driver.dao.VideosByTagDao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

@Mapper
public interface VideosMapper {
  @DaoFactory
  VideosByTagDao videoDao();
}
