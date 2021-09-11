import typing

import numpy as np
import pandas as pd


def find_duplicate_episodes(episodes_df: pd.DataFrame) -> pd.DataFrame:
    """

    :param episodes_df:
    :return:
    """
    episodes_df_ = episodes_df.copy(deep=True)
    episodes_df_ = episodes_df_[episodes_df_.duplicated(subset=['title'], keep=False)]
    return episodes_df_


def find_duplicate_episodes_and_merge_with_xml(episodes_db_df: pd.DataFrame,
                                               episodes_xml_df: pd.DataFrame) -> pd.DataFrame:
    """

    :param episodes_db_df:
    :param episodes_xml_df:
    :return:
    """
    episodes_db_df_ = episodes_db_df.copy(deep=True)
    episodes_db_df_ = episodes_db_df_[episodes_db_df_.duplicated(subset=['title'], keep=False)]
    episodes_merged_df = episodes_db_df_.merge(episodes_xml_df.copy(deep=True), how='left', on='item_identifier')
    return episodes_merged_df


def find_episodes_no_longer_in_xml(episodes_db_df: pd.DataFrame,
                                   episodes_xml_df: pd.DataFrame,
                                   keep_old: bool = True,
                                   keep_played_episodes: bool = True) -> typing.Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Find episodes in the database for one feed which are not in the current xml file of the feed.

    These episodes were seemingly removed by the owner of the feed, which can fall into different categories:
    1.) episodes, which were added accidentally, often also identifiable by uncommon duration or title
    2.) old episodes (if only a certain amount of episodes are included in the xml file).

    :param episodes_db_df: dataframe containing all feed episodes from the database
    :param episodes_xml_df: dataframe containing all feed episodes currently in the xml file
    :param keep_old: boolean flag if old episodes should be kept or not
    :param keep_played_episodes: boolean flag if played episodes should be kept or not
        (beware: influences statistics if not kept)
    :return: dataframe with all episodes no longer in the xml file,
             dataframe with all episodes from db and xml merged
    """
    episodes_merged_df = episodes_db_df.merge(episodes_xml_df, how='outer', on='item_identifier')
    episodes_merged_df.sort_values(by=['pubDate_x'], inplace=True)

    # keep old episodes no longer in xml if required
    if keep_old:
        episodes_merged_df = episodes_merged_df[
            episodes_merged_df['pubDate_x'] >= np.nanmin(episodes_merged_df['pubDate_y'])]

    # filter out new episodes not yet in database
    episodes_merged_df = episodes_merged_df[
        ~(episodes_merged_df['pubDate_y'] >= np.nanmax(episodes_merged_df['pubDate_x']))]

    # filter entries in both
    no_longer_in_xml_df = episodes_merged_df[
        (episodes_merged_df['title_x'] != episodes_merged_df['title_y']) &
        (episodes_merged_df['pubDate_x'] != episodes_merged_df['pubDate_y'])]

    #
    if keep_played_episodes:
        no_longer_in_xml_df = no_longer_in_xml_df[
            (no_longer_in_xml_df['read_x'] != 1)
        ]

    return no_longer_in_xml_df, episodes_merged_df


def find_old_unusually_short_episodes(episodes_db_df: pd.DataFrame,
                                      episodes_xml_df: pd.DataFrame,
                                      media_df: pd.DataFrame) -> typing.Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Find old episodes (no longer included in the xml file), which have an unusually short duration
    and are therefore highly likely episodes, where added by accident.
    Only applicable if the episodes are produced for a certain format with a duration of e.g. one hour.

    The criterion for unusually short is: duration smaller than 2 sigma from the mean of all episodes the database.

    :param episodes_db_df: dataframe containing all feed episodes from the database
    :param episodes_xml_df: dataframe containing all feed episodes currently in the xml file
    :param media_df: dataframe containing all media for all feed episodes from the database
    :return: dataframe containing all episodes which were deemed as too short
             dataframe containing all episodes from db and xml merged, augmented with media information
    """
    episodes_merged_df = episodes_db_df.merge(episodes_xml_df,
                                              how='left',
                                              on='item_identifier',
                                              suffixes=('_db', '_xml'))
    episodes_merged_df = episodes_merged_df[
        episodes_merged_df['pubDate_db'] <= np.nanmin(episodes_merged_df['pubDate_xml'])]

    episodes_media_merged_df = episodes_merged_df.merge(media_df,
                                                        how='left',
                                                        left_on='id',
                                                        right_on='feeditem',
                                                        suffixes=('', '_media'))
    duration_mean = episodes_media_merged_df['duration'].mean()
    duration_std = episodes_media_merged_df['duration'].std()

    two_sigma = duration_mean - 2 * duration_std
    too_short_episodes_df = episodes_media_merged_df[episodes_media_merged_df['duration'] < two_sigma]

    return too_short_episodes_df, episodes_media_merged_df


def prepare_deletion_of_episodes_and_media(episodes_df: pd.DataFrame,
                                           media_df: pd.DataFrame) -> typing.Tuple[typing.List[int], typing.List[int]]:
    """
    Prepare lists of the episodes and the corresponding media to be deleted.

    The dataframe episodes_df should only contain episodes, which are to be deleted,
    as its id column is returned as episodes_to_delete list.
    Using this list of feeditem ids, the corresponding media ids are also returned.

    :param episodes_df: dataframe containing all episodes to be deleted
    :param media_df: dataframe containing all media
    :return: list of feeditem ids to be deleted,
             list of corresponding media ids to be deleted
    """
    episodes_to_delete = episodes_df['id'].tolist()
    episodes_to_delete = [int(epi) for epi in episodes_to_delete]

    media_to_delete = media_df[media_df['feeditem'].isin(episodes_to_delete)]['id'].tolist()
    media_to_delete = [int(md) for md in media_to_delete]

    return episodes_to_delete, media_to_delete
