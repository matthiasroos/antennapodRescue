"""
This script is used to remove episodes in a podcast feed,
which are no longer in the XML file.
It is intended for the removal of episodes which were wrongly inserted into the XML file
or updated afterwards leading to episode duplication in AntennaPod.

The 'keep_old' flag offers the possibility to retain or delete old episodes,
which are no longer in the XML file due to a "episode retirement" custom of the feed owner.
According to such a custom, for example, only a certain amount of episodes
are delivered in the XML file. Old episodes might or not be downloadable from the server.
"""


import sqlmodel_.consolidation
import sqlmodel_.database.delete
import sqlmodel_.database.fetch
import sqlmodel_.utils


if __name__ == '__main__':
    file_name = '<to be specified>'
    feed_number = 72  # needs to be changed accordingly
    keep_old = True

    episodes_xml_df = sqlmodel_.utils.get_episodes_df_for_feed(sqlite_filename=file_name,
                                                               feed_id=feed_number)
    episodes_db_df = sqlmodel_.database.fetch.fetch_episodes_df_from_db(sqlite_filename=file_name,
                                                                        feed_id=feed_number)
    media_df = sqlmodel_.database.fetch.fetch_media_df_from_db(sqlite_filename=file_name,
                                                               feed_id=feed_number)
    episodes_merged_filtered_df, episodes_merged_df = sqlmodel_.consolidation.find_episodes_no_longer_in_xml(
        episodes_db_df=episodes_db_df,
        episodes_xml_df=episodes_xml_df,
        keep_old=keep_old
    )

    episodes_to_delete, media_to_delete = sqlmodel_.consolidation.prepare_deletion_of_episodes_and_media(
        episodes_df=episodes_merged_filtered_df,
        media_df=media_df
    )

    #sqlmodel_.database.delete.delete_feeditems_from_db(sqlite_filename=file_name,
    #                                                   feed_item_ids=episodes_to_delete)

    #sqlmodel_.database.delete.delete_media_from_db(sqlite_filename=file_name,
    #                                               media_ids=media_to_delete)
