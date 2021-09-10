"""
This script is used to remove old episodes (no longer in XML file) in a podcast feed,
which are unusually short.
It is intended for the removal of episodes which were wrongly inserted into the XML file
at that time, but the removal of non-XML episodes is not possible, as all episodes from
the same point in time are no longer included in the XML file.

It is only applicable to feeds with a more or less standardized duration of episodes.
"""
import sqlalchemy_.database.fetch
import sqlmodel_.consolidation
import sqlmodel_.database.delete
import sqlmodel_.database.fetch
import sqlmodel_.utils


if __name__ == '__main__':
    file_name = '<to be insereted>'
    feed_number = 13  # needs to be changed accordingly

    episodes_xml_df = sqlmodel_.utils.get_episodes_df_for_feed(sqlite_filename=file_name,
                                                               feed_id=feed_number)
    episodes_db_df = sqlmodel_.database.fetch.fetch_episodes_df_from_db(sqlite_filename=file_name,
                                                                        feed_id=feed_number)
    media_df = sqlalchemy_.database.fetch.fetch_media_df_from_db(sqlite_filename=file_name,
                                                                 feed_id=feed_number)

    too_short_episodes_df, episodes_media_merged_df = sqlmodel_.consolidation.find_old_unusually_short_episodes(
        episodes_db_df=episodes_db_df,
        episodes_xml_df=episodes_xml_df,
        media_df=media_df)
    episodes_to_delete, media_to_delete = sqlmodel_.consolidation.prepare_deletion_of_episodes_and_media(
        episodes_df=too_short_episodes_df,
        media_df=media_df
    )

    # sqlmodel_.database.delete.delete_feeditems_from_db(sqlite_filename=file_name,
    #                                                    feed_item_ids=episodes_to_delete)

    # sqlmodel_.database.delete.delete_media_from_db(sqlite_filename=file_name,
    #                                                media_ids=media_to_delete)
