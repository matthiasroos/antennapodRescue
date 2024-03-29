-- This file contains SQLite queries specific for the AntennaPod database.


--name: fetch_all_feeds
select id, title, file_url. download_url, downloaded
  from Feeds;

--name: fetch_feed
select id, title, file_url, download_url, downloaded
  from Feeds
 where id = :feed;


--name: fetch_items_from_feed
select id, title, pubDate, "read", link, description, feed, item_identifier, image_url
  from FeedItems
 where feed = :feed;

--name: update_feeditems!
update FeedItems
   set link = :link,
       description = :description,
       item_identifier = :item_identifier,
       image_url = :image_url
 where id = :feeditem_id;

--name: delete_feeditem!
delete from FeedItems
 where id = :feeditem_id;


--name: fetch_media
select duration, download_url, downloaded, filesize, playback_completion_date, feeditem, played_duration, last_played_time
  from FeedMedia
 where feeditem = :feeditem;

--name: update_media!
update FeedMedia
   set download_url = :download_url
 where feeditem = :feeditem;

--name: delete_media!
delete from FeedMedia
 where feeditem = :feeditem;


