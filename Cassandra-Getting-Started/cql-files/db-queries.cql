CREATE KEYSPACE killrvideo WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE videos (
   video_id uuid PRIMARY KEY,
   added_date timestamp,
   title text
   );

INSERT INTO videos (video_id,added_date,title) values (1645ea59-14bd-11e5-a993-8138354b7e31,'2014-01-29','Cassandra History');
INSERT INTO videos (video_id,added_date,title) values (245e8024-14bd-11e5-9743-8238356b7e32,'2012-04-03','Cassandra & SSDs');
INSERT INTO videos (video_id,added_date,title) values (3452f7de-14bd-11e5-855e-8738355b7e3a,'2013-03-17','Cassandra Intro');
INSERT INTO videos (video_id,added_date,title) values (4845ed97-14bd-11e5-8a40-8338255b7e33,'2013-10-16','DataStax DevCenter');  
INSERT INTO videos (video_id,added_date,title) values (5645f8bd-14bd-11e5-af1a-8638355b8e3a,'2013-04-16','What is DataStax Enterprise?');   


COPY videos(video_id, added_date, title) FROM 'C:\\Users\\s.u.nagarajan\\Shravan\\Learning\\Cassandra\\ds201-Foundation of Apache Cassandra\\ds201-6.0-labwork\\labwork\\data-files\\videos.csv' WITH HEADER=TRUE;


CREATE TABLE videos_by_tag (
   tag text,
   video_id uuid,
   added_date timestamp,
   title text,
   PRIMARY KEY ((tag), video_id) 
   );

COPY videos_by_tag(tag, video_id, added_date, title) FROM 'C:\\Users\\s.u.nagarajan\\Shravan\\Learning\\Cassandra\\ds201-Foundation of Apache Cassandra\\ds201-6.0-labwork\\labwork\\data-files\\videos-by-tag.csv' WITH HEADER = TRUE;


CREATE TABLE videos_by_tag (
     tag text,
     video_id uuid,
     added_date timestamp,
     title text,
     PRIMARY KEY ((tag),added_date,video_id) )
	 WITH CLUSTERING ORDER BY(added_date DESC);

COPY videos_by_tag(tag, video_id, added_date, title) FROM 'C:\\Users\\s.u.nagarajan\\Shravan\\Learning\\Cassandra\\ds201-Foundation of Apache Cassandra\\ds201-6.0-labwork\\labwork\\data-files\\videos-by-tag.csv' WITH HEADER = TRUE;

select * from videos_by_tag where tag = 'cassandra' and  added_date < '2013-01-01';

