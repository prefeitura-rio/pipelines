"""
Tasks for twitter scraping.
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################
import base64
import os
import time
import json
from datetime import datetime

import shutil
import pandas as pd
import numpy as np
import basedosdados as bd
import tweepy

from prefect import task

from pipelines.utils import log


def decode_env(value: str):
    """
    Decodes a base64 value.
    """
    return base64.b64decode(value).decode()


@task
def get_api():
    """
    Get the Twitter API.
    """

    # pylint: disable=C0103
    CREDENTIALS = json.loads(decode_env(os.getenv("TWITTER_CREDENTIALS")))

    auth = tweepy.OAuthHandler(
        CREDENTIALS["CONSUMER_KEY"], CREDENTIALS["CONSUMER_SECRET"]
    )
    auth.set_access_token(
        CREDENTIALS["ACCESS_TOKEN"], CREDENTIALS["ACCESS_TOKEN_SECRET"]
    )
    return tweepy.API(auth)


def normalize_cols(df):  # pylint: disable=C0103
    """
    Normalize columns names.
    """
    return (
        df.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.replace("$", "")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace("-", "")
        .str.replace(" ", "_")
        .str.lower()
        .str.replace(".", "")
        .str.replace("/", "_")
    )


def creat_path_tree(path):
    """
    Creates a path tree.
    """
    current_path = ""
    for folder in path.split("/"):
        current_path += f"{folder}/"
        if folder != ".." and not os.path.isdir(current_path):
            os.mkdir(current_path)


@task
def save_last_id(df, q):  # pylint: disable=C0103
    """
    Save the last tweet ID.
    """
    q_folder = q.replace(" ", "_").replace("-", "_")
    pre_path = f"data/staging/twitter_flamengo/last_id/q={q_folder}"
    creat_path_tree(pre_path)

    if not os.path.exists(f"{pre_path}/{q_folder}.csv"):
        pd.DataFrame({"q": [], "id": [], "created_at": []}).to_csv(
            f"{pre_path}/{q_folder}.csv", index=False
        )

    if df is None:
        log("   No new tweets found")
    else:
        # twitter fetch data from most recent to most oldest
        # last_id must to be the most recent id
        df = df[["id", "created_at"]].iloc[[0]].copy()
        df.to_csv(f"{pre_path}/{q_folder}.csv",
                  index=False, mode="a", header=False)
    return "data/staging"


@task
def fetch_last_id(q):  # pylint: disable=C0103
    """
    Download last_id table from storage.
    """
    q_folder = q.replace(" ", "_").replace("-", "_")
    # pylint: disable=C0103
    st = bd.Storage(
        dataset_id="twitter_flamengo", table_id="last_id",
    )
    try:
        st.download(
            filename=f"{q_folder}.csv",
            savepath="data",
            partitions=f"q={q_folder}/",
            mode="staging",
            if_not_exists="raise",
        )

        pre_path = f"data/staging/twitter_flamengo/last_id/q={q_folder}"
        df = pd.read_csv(f"{pre_path}/{q_folder}.csv")  # pylint: disable=C0103
        if "q" in df.columns.tolist():
            df.columns = ["id", "created_at", "q"]
            df.drop("q", 1).to_csv(  # pylint: disable=E1101
                f"{pre_path}/{q_folder}.csv", index=False
            )
    except FileNotFoundError:
        log(f"No table {q_folder} in storage")

    return "data/staging"


@task(nout=2)
def get_last_id(api, q, data_path: str):  # pylint: disable=C0103
    """
    Get last_id from storage table or twitter api.
    """
    q_folder = q.replace(" ", "_").replace("-", "_")
    pre_path = f"{data_path}/twitter_flamengo/last_id/q={q_folder}"
    if not os.path.exists(f"{pre_path}/{q_folder}.csv"):
        log(f"No last_id table found for {q}, fetch last_id from Twitter API")
        tweet = api.search_tweets(q=q, count=1)[0]
        last_id = tweet.id
        created_at = tweet.created_at
        time.sleep(5)
    else:
        df = pd.read_csv(  # pylint: disable=C0103
            f"{pre_path}/{q_folder}.csv").copy()
        if len(df) > 0:
            log(f"Getting last_id from storage table {q_folder}")
            last_id = int(df[["id"]].iloc[-1])
            created_at = df[["created_at"]].iloc[-1].values[0]
        else:
            log(
                f"No last_id saved in table for {q}, fetch last_id from Twitter API")
            tweet = api.search_tweets(q=q, count=1)[0]
            last_id = tweet.id
            created_at = tweet.created_at
            time.sleep(5)
    return last_id, created_at


@task
def fetch_tweets(api, q, last_id, created_at):  # pylint: disable=C0103
    """
    Scrapy tweets since last_id in batchs of 100 tweets.
    """
    q_folder = q.replace(" ", "_").replace("-", "_")
    dt = datetime.today().strftime("%Y-%m-%d-%H-%M-%S")  # pylint: disable=C0103
    first_page_df = None
    log(f"{q} | last_id: {last_id} | created_at: {created_at} | file: {dt}")

    for i, page in enumerate(
        tweepy.Cursor(api.search_tweets, q=q,
                      since_id=last_id, count=100).pages(100),
        start=1,
    ):
        json_data = [t._json for t in page]  # pylint: disable=W0212
        dd = pd.json_normalize(json_data)  # pylint: disable=C0103
        dd.columns = normalize_cols(dd.columns)

        cols_343 = [
            "created_at",
            "id",
            "id_str",
            "text",
            "truncated",
            "source",
            "in_reply_to_status_id",
            "in_reply_to_status_id_str",
            "in_reply_to_user_id",
            "in_reply_to_user_id_str",
            "in_reply_to_screen_name",
            "geo",
            "coordinates",
            "place",
            "contributors",
            "is_quote_status",
            "retweet_count",
            "favorite_count",
            "favorited",
            "retweeted",
            "lang",
            "entitieshashtags",
            "entitiessymbols",
            "entitiesuser_mentions",
            "entitiesurls",
            "metadataiso_language_code",
            "metadataresult_type",
            "userid",
            "userid_str",
            "username",
            "userscreen_name",
            "userlocation",
            "userdescription",
            "userurl",
            "userentitiesurlurls",
            "userentitiesdescriptionurls",
            "userprotected",
            "userfollowers_count",
            "userfriends_count",
            "userlisted_count",
            "usercreated_at",
            "userfavourites_count",
            "userutc_offset",
            "usertime_zone",
            "usergeo_enabled",
            "userverified",
            "userstatuses_count",
            "userlang",
            "usercontributors_enabled",
            "useris_translator",
            "useris_translation_enabled",
            "userprofile_background_color",
            "userprofile_background_image_url",
            "userprofile_background_image_url_https",
            "userprofile_background_tile",
            "userprofile_image_url",
            "userprofile_image_url_https",
            "userprofile_banner_url",
            "userprofile_link_color",
            "userprofile_sidebar_border_color",
            "userprofile_sidebar_fill_color",
            "userprofile_text_color",
            "userprofile_use_background_image",
            "userhas_extended_profile",
            "userdefault_profile",
            "userdefault_profile_image",
            "userfollowing",
            "userfollow_request_sent",
            "usernotifications",
            "usertranslator_type",
            "userwithheld_in_countries",
            "retweeted_statuscreated_at",
            "retweeted_statusid",
            "retweeted_statusid_str",
            "retweeted_statustext",
            "retweeted_statustruncated",
            "retweeted_statusentitieshashtags",
            "retweeted_statusentitiessymbols",
            "retweeted_statusentitiesuser_mentions",
            "retweeted_statusentitiesurls",
            "retweeted_statusmetadataiso_language_code",
            "retweeted_statusmetadataresult_type",
            "retweeted_statussource",
            "retweeted_statusin_reply_to_status_id",
            "retweeted_statusin_reply_to_status_id_str",
            "retweeted_statusin_reply_to_user_id",
            "retweeted_statusin_reply_to_user_id_str",
            "retweeted_statusin_reply_to_screen_name",
            "retweeted_statususerid",
            "retweeted_statususerid_str",
            "retweeted_statususername",
            "retweeted_statususerscreen_name",
            "retweeted_statususerlocation",
            "retweeted_statususerdescription",
            "retweeted_statususerurl",
            "retweeted_statususerentitiesdescriptionurls",
            "retweeted_statususerprotected",
            "retweeted_statususerfollowers_count",
            "retweeted_statususerfriends_count",
            "retweeted_statususerlisted_count",
            "retweeted_statususercreated_at",
            "retweeted_statususerfavourites_count",
            "retweeted_statususerutc_offset",
            "retweeted_statususertime_zone",
            "retweeted_statususergeo_enabled",
            "retweeted_statususerverified",
            "retweeted_statususerstatuses_count",
            "retweeted_statususerlang",
            "retweeted_statususercontributors_enabled",
            "retweeted_statususeris_translator",
            "retweeted_statususeris_translation_enabled",
            "retweeted_statususerprofile_background_color",
            "retweeted_statususerprofile_background_image_url",
            "retweeted_statususerprofile_background_image_url_https",
            "retweeted_statususerprofile_background_tile",
            "retweeted_statususerprofile_image_url",
            "retweeted_statususerprofile_image_url_https",
            "retweeted_statususerprofile_banner_url",
            "retweeted_statususerprofile_link_color",
            "retweeted_statususerprofile_sidebar_border_color",
            "retweeted_statususerprofile_sidebar_fill_color",
            "retweeted_statususerprofile_text_color",
            "retweeted_statususerprofile_use_background_image",
            "retweeted_statususerhas_extended_profile",
            "retweeted_statususerdefault_profile",
            "retweeted_statususerdefault_profile_image",
            "retweeted_statususerfollowing",
            "retweeted_statususerfollow_request_sent",
            "retweeted_statususernotifications",
            "retweeted_statususertranslator_type",
            "retweeted_statususerwithheld_in_countries",
            "retweeted_statusgeo",
            "retweeted_statuscoordinates",
            "retweeted_statusplace",
            "retweeted_statuscontributors",
            "retweeted_statusis_quote_status",
            "retweeted_statusretweet_count",
            "retweeted_statusfavorite_count",
            "retweeted_statusfavorited",
            "retweeted_statusretweeted",
            "retweeted_statuslang",
            "possibly_sensitive",
            "entitiesmedia",
            "extended_entitiesmedia",
            "retweeted_statusentitiesmedia",
            "retweeted_statusextended_entitiesmedia",
            "retweeted_statususerentitiesurlurls",
            "retweeted_statuspossibly_sensitive",
            "placeid",
            "placeurl",
            "placeplace_type",
            "placename",
            "placefull_name",
            "placecountry_code",
            "placecountry",
            "placecontained_within",
            "placebounding_boxtype",
            "placebounding_boxcoordinates",
            "quoted_status_id",
            "quoted_status_id_str",
            "quoted_statuscreated_at",
            "quoted_statusid",
            "quoted_statusid_str",
            "quoted_statustext",
            "quoted_statustruncated",
            "quoted_statusentitieshashtags",
            "quoted_statusentitiessymbols",
            "quoted_statusentitiesuser_mentions",
            "quoted_statusentitiesurls",
            "quoted_statusentitiesmedia",
            "quoted_statusextended_entitiesmedia",
            "quoted_statusmetadataiso_language_code",
            "quoted_statusmetadataresult_type",
            "quoted_statussource",
            "quoted_statusin_reply_to_status_id",
            "quoted_statusin_reply_to_status_id_str",
            "quoted_statusin_reply_to_user_id",
            "quoted_statusin_reply_to_user_id_str",
            "quoted_statusin_reply_to_screen_name",
            "quoted_statususerid",
            "quoted_statususerid_str",
            "quoted_statususername",
            "quoted_statususerscreen_name",
            "quoted_statususerlocation",
            "quoted_statususerdescription",
            "quoted_statususerurl",
            "quoted_statususerentitiesurlurls",
            "quoted_statususerentitiesdescriptionurls",
            "quoted_statususerprotected",
            "quoted_statususerfollowers_count",
            "quoted_statususerfriends_count",
            "quoted_statususerlisted_count",
            "quoted_statususercreated_at",
            "quoted_statususerfavourites_count",
            "quoted_statususerutc_offset",
            "quoted_statususertime_zone",
            "quoted_statususergeo_enabled",
            "quoted_statususerverified",
            "quoted_statususerstatuses_count",
            "quoted_statususerlang",
            "quoted_statususercontributors_enabled",
            "quoted_statususeris_translator",
            "quoted_statususeris_translation_enabled",
            "quoted_statususerprofile_background_color",
            "quoted_statususerprofile_background_image_url",
            "quoted_statususerprofile_background_image_url_https",
            "quoted_statususerprofile_background_tile",
            "quoted_statususerprofile_image_url",
            "quoted_statususerprofile_image_url_https",
            "quoted_statususerprofile_banner_url",
            "quoted_statususerprofile_link_color",
            "quoted_statususerprofile_sidebar_border_color",
            "quoted_statususerprofile_sidebar_fill_color",
            "quoted_statususerprofile_text_color",
            "quoted_statususerprofile_use_background_image",
            "quoted_statususerhas_extended_profile",
            "quoted_statususerdefault_profile",
            "quoted_statususerdefault_profile_image",
            "quoted_statususerfollowing",
            "quoted_statususerfollow_request_sent",
            "quoted_statususernotifications",
            "quoted_statususertranslator_type",
            "quoted_statususerwithheld_in_countries",
            "quoted_statusgeo",
            "quoted_statuscoordinates",
            "quoted_statusplaceid",
            "quoted_statusplaceurl",
            "quoted_statusplaceplace_type",
            "quoted_statusplacename",
            "quoted_statusplacefull_name",
            "quoted_statusplacecountry_code",
            "quoted_statusplacecountry",
            "quoted_statusplacecontained_within",
            "quoted_statusplacebounding_boxtype",
            "quoted_statusplacebounding_boxcoordinates",
            "quoted_statuscontributors",
            "quoted_statusis_quote_status",
            "quoted_statusretweet_count",
            "quoted_statusfavorite_count",
            "quoted_statusfavorited",
            "quoted_statusretweeted",
            "quoted_statuspossibly_sensitive",
            "quoted_statuslang",
            "retweeted_statusplaceid",
            "retweeted_statusplaceurl",
            "retweeted_statusplaceplace_type",
            "retweeted_statusplacename",
            "retweeted_statusplacefull_name",
            "retweeted_statusplacecountry_code",
            "retweeted_statusplacecountry",
            "retweeted_statusplacecontained_within",
            "retweeted_statusplacebounding_boxtype",
            "retweeted_statusplacebounding_boxcoordinates",
            "retweeted_statusquoted_status_id",
            "retweeted_statusquoted_status_id_str",
            "retweeted_statusquoted_statuscreated_at",
            "retweeted_statusquoted_statusid",
            "retweeted_statusquoted_statusid_str",
            "retweeted_statusquoted_statustext",
            "retweeted_statusquoted_statustruncated",
            "retweeted_statusquoted_statusentitieshashtags",
            "retweeted_statusquoted_statusentitiessymbols",
            "retweeted_statusquoted_statusentitiesuser_mentions",
            "retweeted_statusquoted_statusentitiesurls",
            "retweeted_statusquoted_statusentitiesmedia",
            "retweeted_statusquoted_statusextended_entitiesmedia",
            "retweeted_statusquoted_statusmetadataiso_language_code",
            "retweeted_statusquoted_statusmetadataresult_type",
            "retweeted_statusquoted_statussource",
            "retweeted_statusquoted_statusin_reply_to_status_id",
            "retweeted_statusquoted_statusin_reply_to_status_id_str",
            "retweeted_statusquoted_statusin_reply_to_user_id",
            "retweeted_statusquoted_statusin_reply_to_user_id_str",
            "retweeted_statusquoted_statusin_reply_to_screen_name",
            "retweeted_statusquoted_statususerid",
            "retweeted_statusquoted_statususerid_str",
            "retweeted_statusquoted_statususername",
            "retweeted_statusquoted_statususerscreen_name",
            "retweeted_statusquoted_statususerlocation",
            "retweeted_statusquoted_statususerdescription",
            "retweeted_statusquoted_statususerurl",
            "retweeted_statusquoted_statususerentitiesurlurls",
            "retweeted_statusquoted_statususerentitiesdescriptionurls",
            "retweeted_statusquoted_statususerprotected",
            "retweeted_statusquoted_statususerfollowers_count",
            "retweeted_statusquoted_statususerfriends_count",
            "retweeted_statusquoted_statususerlisted_count",
            "retweeted_statusquoted_statususercreated_at",
            "retweeted_statusquoted_statususerfavourites_count",
            "retweeted_statusquoted_statususerutc_offset",
            "retweeted_statusquoted_statususertime_zone",
            "retweeted_statusquoted_statususergeo_enabled",
            "retweeted_statusquoted_statususerverified",
            "retweeted_statusquoted_statususerstatuses_count",
            "retweeted_statusquoted_statususerlang",
            "retweeted_statusquoted_statususercontributors_enabled",
            "retweeted_statusquoted_statususeris_translator",
            "retweeted_statusquoted_statususeris_translation_enabled",
            "retweeted_statusquoted_statususerprofile_background_color",
            "retweeted_statusquoted_statususerprofile_background_image_url",
            "retweeted_statusquoted_statususerprofile_background_image_url_https",
            "retweeted_statusquoted_statususerprofile_background_tile",
            "retweeted_statusquoted_statususerprofile_image_url",
            "retweeted_statusquoted_statususerprofile_image_url_https",
            "retweeted_statusquoted_statususerprofile_banner_url",
            "retweeted_statusquoted_statususerprofile_link_color",
            "retweeted_statusquoted_statususerprofile_sidebar_border_color",
            "retweeted_statusquoted_statususerprofile_sidebar_fill_color",
            "retweeted_statusquoted_statususerprofile_text_color",
            "retweeted_statusquoted_statususerprofile_use_background_image",
            "retweeted_statusquoted_statususerhas_extended_profile",
            "retweeted_statusquoted_statususerdefault_profile",
            "retweeted_statusquoted_statususerdefault_profile_image",
            "retweeted_statusquoted_statususerfollowing",
            "retweeted_statusquoted_statususerfollow_request_sent",
            "retweeted_statusquoted_statususernotifications",
            "retweeted_statusquoted_statususertranslator_type",
            "retweeted_statusquoted_statususerwithheld_in_countries",
            "retweeted_statusquoted_statusgeo",
            "retweeted_statusquoted_statuscoordinates",
            "retweeted_statusquoted_statusplaceid",
            "retweeted_statusquoted_statusplaceurl",
            "retweeted_statusquoted_statusplaceplace_type",
            "retweeted_statusquoted_statusplacename",
            "retweeted_statusquoted_statusplacefull_name",
            "retweeted_statusquoted_statusplacecountry_code",
            "retweeted_statusquoted_statusplacecountry",
            "retweeted_statusquoted_statusplacecontained_within",
            "retweeted_statusquoted_statusplacebounding_boxtype",
            "retweeted_statusquoted_statusplacebounding_boxcoordinates",
            "retweeted_statusquoted_statuscontributors",
            "retweeted_statusquoted_statusis_quote_status",
            "retweeted_statusquoted_statusretweet_count",
            "retweeted_statusquoted_statusfavorite_count",
            "retweeted_statusquoted_statusfavorited",
            "retweeted_statusquoted_statusretweeted",
            "retweeted_statusquoted_statuspossibly_sensitive",
            "retweeted_statusquoted_statuslang",
            "quoted_statusplace",
            "geotype",
            "geocoordinates",
            "coordinatestype",
            "coordinatescoordinates",
        ]
        col_not_in_dd = [
            col for col in cols_343 if col not in dd.columns.tolist()]
        for col in col_not_in_dd:
            dd[col] = np.nan
        dd = dd[cols_343]  # pylint: disable=C0103

        creat_path_tree(f"data/tweets/q={q_folder}")
        if os.path.exists(f"data/tweets/q={q_folder}/{dt}.csv"):
            dd.to_csv(
                f"data/tweets/q={q_folder}/{dt}.csv",
                index=False,
                mode="a",
                header=False,
            )
        else:
            dd.to_csv(f"data/tweets/q={q_folder}/{dt}.csv", index=False)
            # twitter fetch data from most recent to most oldest
            # the first tweet from first page is the most recent, how its what we call last_id
            first_page_df = dd.copy()

        log(f"    page: {i} | tweets: {len(dd)} | columns: {len(dd.columns)}")

    return first_page_df


@task
def upload_to_storage(path: str):
    """
    upload data to storage
    """
    tb_last_id = bd.Table(dataset_id="twitter_flamengo", table_id="last_id")
    tb_last_id.append(f"{path}/twitter_flamengo/last_id")
    if os.path.isdir(f"{path}/"):
        shutil.rmtree(f"{path}/")

    tb_tweet = bd.Table(dataset_id="twitter_flamengo", table_id="tweets")
    tb_tweet.append(
        "data/tweets",
    )
    if os.path.isdir("data/tweets/"):
        shutil.rmtree("data/tweets/")
