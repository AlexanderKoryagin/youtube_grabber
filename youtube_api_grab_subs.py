"""
Script to grab videos with generated subtitles from YouTube channel.

You can get 'client_secret.json' from:
    https://console.developers.google.com/

Remember about quotas:
    The quota will be reset at midnight Pacific Time (PST) \\ -11 hours to Moscow time
    Queries per day = 10 000
    Usage: https://console.developers.google.com/apis/api/youtube/usage
"""
import glob
import os
import re
from collections import namedtuple

import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


CHANNEL_ID = "UCU4EZpLc84IZMFnpp1TyiKg"  # SiliconValleyVoice

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
CLIENT_SECRETS_FILE = "client_secret_3.json"
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"


Video = namedtuple("Video", "uploaded, video_id, title, sub_idx, sub_start, sub_end, sub_text")
SubtitleMsg = namedtuple("SubtitleLine", "idx, start, end, text")


def get_authenticated_service():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_console()
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


class VideoGetter:
    """
    9985 units cost for processing 48 videos (9 videos with no caption)
    9985 // (48 - 9) = ~ 256 units per one video
    9981 // (44 - 1) = ~ 232 units per one video
    10042 // 40      = ~ 251 units per one video
    """

    DIR_NAME = "by_title"
    GRAB_BEFORE = "2018-10-20T00:00:00Z"
    GRAB_AFTER = "2018-01-01T00:00:00Z"

    def __init__(self, client, get_after_least: bool = True, get_latest: bool = False):
        self.client = client
        self.get_latest = get_latest
        self.get_after_least = get_after_least
        self.create_dir()
        self._get_search_videos()

    def create_dir(self):
        if not os.path.exists(self.DIR_NAME):
            os.makedirs(self.DIR_NAME)

    def get_extreme_video_date(self):
        dates = []
        for one_file in glob.glob(f"{self.DIR_NAME}/*.csv"):
            file_name = one_file.split("/", 1)[1]
            date = "_".join(file_name.split("_", 2)[:2])
            dates.append(pd.to_datetime(date, format="%Y%m%d_%H%M%S", utc=True))
        if dates:
            return {"last": max(dates), "oldest": min(dates)}
        return None  # TODO raise?

    def _get_search_videos(self, page_token: str = None):
        """
        https://developers.google.com/youtube/v3/docs/search/list
        cost: 100 units
        """
        # get latest missing videos
        if self.get_latest:
            latest_video_date = self.get_extreme_video_date()["last"]
            self.GRAB_AFTER = latest_video_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            self.GRAB_BEFORE = None

        # get old videos starting from existing
        if self.get_after_least:
            oldest_video_date = self.get_extreme_video_date()["oldest"]
            self.GRAB_AFTER = None
            self.GRAB_BEFORE = oldest_video_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        response = (
            self.client.search()
            .list(
                part="snippet",
                channelId=CHANNEL_ID,
                maxResults=50,
                order="date",
                publishedAfter=self.GRAB_AFTER,
                publishedBefore=self.GRAB_BEFORE,
                safeSearch=None,
                type="video",
                pageToken=page_token,
            )
            .execute()
        )
        # for every video
        for item in response["items"]:
            video_subs = []
            # get video info
            uploaded_time = pd.Timestamp(item["snippet"]["publishedAt"])
            video_id = item["id"]["videoId"]
            title = item["snippet"]["title"]
            print(f"Processing video: {uploaded_time} - {title}")

            asr_caption_id = self.get_asr_caption_id(item["id"]["videoId"])
            if not asr_caption_id:
                print(f"\tvideo has no ASR caption")
                continue

            subtitles = self.get_subtitles(asr_caption_id)
            for subtitle_line in subtitles:
                video_subs.append(
                    Video(
                        uploaded=uploaded_time,
                        video_id=video_id,
                        title=title,
                        sub_idx=subtitle_line.idx,
                        sub_start=subtitle_line.start,
                        sub_end=subtitle_line.end,
                        sub_text=subtitle_line.text,
                    )
                )

            # save to CSV every video
            if video_subs:
                timestamp = uploaded_time.strftime("%Y%m%d_%H%M%S")
                pd_video_subs = pd.DataFrame(video_subs)
                pd_video_subs[["video_id", "title", "sub_start", "sub_text"]].to_csv(
                    path_or_buf=f"{self.DIR_NAME}/{timestamp}_{video_id}.csv",
                    encoding="utf-8",
                    index=False,
                )

        next_page_token = response.get("nextPageToken", None)
        if next_page_token:
            self._get_search_videos(page_token=next_page_token)

    def get_asr_caption_id(self, video_id: str = None):
        """
        https://developers.google.com/youtube/v3/docs/captions/list
        cost: 50 units
        """
        response = self.client.captions().list(part="snippet", videoId=video_id).execute()
        for item in response["items"]:
            # ASR – A caption track generated using automatic speech recognition
            if item["snippet"]["trackKind"] == "ASR":
                return item["id"]
        return None

    def get_subtitles(self, caption_id: str):
        """
        https://developers.google.com/youtube/v3/docs/captions/download
        cost: 200 units
        """
        pattern = re.compile(
            r"(?P<idx>\d+)\n(?P<start>[\d+|:|,]+) --> (?P<end>[\d+|:|,]+)\n(?P<text>.*)"
        )
        try:
            subtitle = self.client.captions().download(id=caption_id, tfmt="srt").execute()
            subtitle = subtitle.decode("utf-8")

            result = []
            for line in subtitle.split("\n\n"):
                line = line.strip()
                if not line:
                    continue
                search_res = re.search(pattern, line)
                if search_res:
                    result.append(SubtitleMsg(**search_res.groupdict()))
            return result

        except HttpError as e:
            if int(e.resp["status"]) == 403:  # forbidden
                return []
            raise e


def main():
    youtube_client = get_authenticated_service()
    VideoGetter(youtube_client)

    # youtube_client = None
    # VideoGetter(youtube_client, get_latest=True)

    print(1)
    exit(1)

    # Load
    pd_videos = pd.read_csv("Videos_all_20190124_1108.csv")
    pd_videos["uploaded"] = pd_videos["uploaded"].apply(pd.Timestamp)

    # Other experiments
    pd_videos["sub_text"].str.split()
    pd_videos["sub_text"].str.split().apply(pd.Series).stack()

    #
    (
        pd_videos["sub_text"].str.split().apply(pd.Series)
        .merge(pd_videos, left_index=True, right_index=True)
        .drop(["sub_text"], axis=1)
        # .melt(
        #     id_vars=['uploaded', 'video_id', 'title', 'sub_idx', 'sub_start', 'sub_end'],
        #     value_name="sub_text"
        # )
    )

    # # split to files
    # import os
    # dir_name = "by_title"
    # if not os.path.exists(dir_name):
    #     os.makedirs(dir_name)
    # for (uploaded, video_id), pd_group in pd_videos.groupby(["uploaded", "video_id"]):
    #     uploaded_str = uploaded.strftime("%Y%m%d_%H%M%S")
    #     pd_group[["video_id", "title", "sub_start", "sub_text"]].to_csv(
    #         f"{dir_name}/{uploaded_str}_{video_id}.csv",
    #         encoding="utf-8",
    #         index=False
    #     )

    # names = namedtuple('Names', 'a, b, values')
    # values = namedtuple('Values', 'one, two')
    # all_names = [
    #     names(a=x, b=x+1, values=[values(i, i+1) for i in range(2)])
    #     for x in range(5, 8)
    # ]
    # all_names_pd = pd.DataFrame(all_names)
    # print(all_names_pd)


if __name__ == "__main__":
    main()
