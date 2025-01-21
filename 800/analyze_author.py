import argparse
import concurrent.futures
import os
import sys
from datetime import datetime
from logging import basicConfig, getLogger

import polars as pl
from mongoengine import connect
from pycoshark.mongomodels import (
    Commit,
    File,
    FileAction,
    Project,
    PullRequest,
    PullRequestComment,
    PullRequestCommit,
    PullRequestFile,
    PullRequestReview,
    PullRequestReviewComment,
    PullRequestSystem,
    VCSSystem,
)
from pycoshark.utils import create_mongodb_uri_string

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.timeit_decorator import timeit_decorator

# 定数
DEPENDABOT_PREFIX = "Bump "
DEPENDABOT_UNIQUE_SUBSTRING = (
    "You can trigger Dependabot actions by commenting on this PR:"
)

# ロギングの設定
basicConfig(
    level="INFO",
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)
logger = getLogger(__name__)


def connect_to_mongodb():
    uri = create_mongodb_uri_string(
        db_user=os.getenv("SMARTSHARK_DB_USERNAME"),
        db_password=os.getenv("SMARTSHARK_DB_PASSWORD"),
        db_hostname=os.getenv("SMARTSHARK_DB_HOST"),
        db_port=os.getenv("SMARTSHARK_DB_PORT"),
        db_authentication_database=os.getenv("SMARTSHARK_DB_AUTHENTICATION_DATABASE"),
        db_ssl_enabled=False,
    )
    return connect(
        db=os.getenv("SMARTSHARK_DB_DATABASE"),
        host=uri,
    )


def worker(project: Project) -> pl.DataFrame:
    logger.info(f"{project.name} Start")

    client = connect_to_mongodb()
    row = process_project(project)
    client.close()

    return pl.DataFrame(row)


def process_project(project: Project) -> dict:
    vcs_system_id = VCSSystem.objects(project_id=project.id).first().id
    pull_request_systems = PullRequestSystem.objects(project_id=project.id).only(
        "id", "url"
    )
    pull_request_system_ids = [
        pull_request_system.id for pull_request_system in pull_request_systems
    ]

    # マージされた pull_request を取得
    pull_requests: list[PullRequest] = PullRequest.objects(
        pull_request_system_id__in=pull_request_system_ids, merged_at__exists=True
    ).only(
        "id",
        "title",
        "description",
        "creator_id",
    )

    row = {
        "project": project.name,
        "bot_ids": set(),
    }

    for pull_request in pull_requests:
        # dependabot によるプルリクエストか判定
        if (
            pull_request.title.startswith(DEPENDABOT_PREFIX)
            and DEPENDABOT_UNIQUE_SUBSTRING in pull_request.description
        ):
            row["bot_ids"].add(str(pull_request.creator_id))

    # bot_ids を文字列に変換
    row["bot_ids"] = ",".join(list(row["bot_ids"]))

    return row


@timeit_decorator(logger=logger)
def main(args):
    # データベースに接続
    client = connect_to_mongodb()

    # データフレームの初期化
    df = pl.DataFrame()

    # プロジェクトの取得
    projects: list[Project] = Project.objects()

    # 並行実行のためにデータベース接続を閉じる
    client.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        done_count = 0
        future_to_project = {
            executor.submit(worker, project): project for project in projects
        }
        for future in concurrent.futures.as_completed(future_to_project):
            # 進捗表示
            done_count += 1
            project = future_to_project[future]
            logger.info(f"{project.name} Done ({done_count}/{len(projects)})")
            # データフレームに追加
            df = pl.concat([df, future.result()], how="diagonal")

    # CSV 出力
    with open(args.output, "w") as f:
        df.write_csv(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", type=str, default="data/author_info.csv")
    args = parser.parse_args()
    main(args)
