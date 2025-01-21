import argparse
import concurrent.futures
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from logging import basicConfig, getLogger

import polars as pl
from mongoengine import connect
from pycoshark.mongomodels import (
    Commit,
    FileAction,
    Project,
    PullRequest,
    PullRequestComment,
    PullRequestCommit,
    PullRequestReview,
    PullRequestReviewComment,
    PullRequestSystem,
    Refactoring,
    VCSSystem,
)
from pycoshark.utils import create_mongodb_uri_string

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.timeit_decorator import timeit_decorator

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
    # 進捗表示
    logger.info(f"{project.name} start.")

    client = connect_to_mongodb()
    row = process_project(project)
    client.close()
    return pl.DataFrame(row)


def process_project(project: Project) -> dict:
    row = defaultdict(int)

    # vcs_sustem を取得
    vcs_system: VCSSystem = VCSSystem.objects(project_id=project.id).first()

    # プルリクエストシステムの id のリストを取得
    pull_request_system: PullRequestSystem = PullRequestSystem.objects(
        project_id=project.id
    )
    pull_request_system_ids = [prs.id for prs in pull_request_system]

    # プロジェクト名
    row["project"] = project.name

    # プルリクエストのカウント
    row["#pull_request"] = PullRequest.objects(
        pull_request_system_id__in=pull_request_system_ids
    ).count()

    # プルリクエストの日付の最小値と最大値を取得
    first_pull_request = (
        PullRequest.objects(pull_request_system_id__in=pull_request_system_ids)
        .only("created_at")
        .order_by("+created_at")
        .first()
    )
    last_pull_request = (
        PullRequest.objects(pull_request_system_id__in=pull_request_system_ids)
        .only("created_at")
        .order_by("-created_at")
        .first()
    )
    row["first_pull_request_date"] = (
        first_pull_request.created_at if first_pull_request else None
    )
    row["last_pull_request_date"] = (
        last_pull_request.created_at if last_pull_request else None
    )

    # コミットと紐づけることができるプルリクエストのカウント
    row["last_commit_date"] = (
        Commit.objects(vcs_system_id=vcs_system.id)
        .only("committer_date")
        .order_by("-committer_date")
        .first()
    ).committer_date
    row["#pull_request_with_commit"] = PullRequest.objects(
        pull_request_system_id__in=pull_request_system_ids,
        created_at__lte=row["last_commit_date"] + timedelta(days=7),
    ).count()

    # マージされたプルリクエストのカウント
    row["#merged_pull_request"] = PullRequest.objects(
        pull_request_system_id__in=pull_request_system_ids,
        merged_at__exists=True,
    ).count()

    # マージされたプルリクエスト毎に処理
    pull_requests: list[PullRequest] = PullRequest.objects(
        pull_request_system_id__in=pull_request_system_ids,
        merged_at__exists=True,
    ).only("id")
    for pull_request in pull_requests:
        # pull_request に紐づいたコミット id のリストを取得
        pull_request_commits: list[PullRequestCommit] = PullRequestCommit.objects(
            pull_request_id=pull_request.id
        ).only("commit_id")
        commit_ids = [pc.commit_id for pc in pull_request_commits]

        # commit_ids の欠損度合いをカウント
        row["#mpr_all"] += all(commit_ids)
        row["#mpr_partial"] += any(commit_ids) and not all(commit_ids)
        row["#mpr_none"] += not any(commit_ids)

        # commit_ids が欠損している場合はスキップ
        if None in commit_ids:
            continue

        # commit のリストを取得
        commits: list[Commit] = Commit.objects(id__in=commit_ids).only("id", "labels")

        # commit 数の度数分布を取得
        row["#mpr_nc==0"] += len(commits) == 0
        row["#mpr_nc==1"] += len(commits) == 1
        row["#mpr_1<nc<=5"] += 1 <= len(commits) <= 5
        row["#mpr_5<nc<=10"] += 5 < len(commits) <= 10
        row["#mpr_10<nc<=20"] += 10 < len(commits) <= 20
        row["#mpr_20<nc<=30"] += 20 < len(commits) <= 30
        row["#mpr_30<nc"] += 30 < len(commits)

    return row


@timeit_decorator(logger=logger)
def main(args):
    # データベースに接続
    client = connect_to_mongodb()

    # データフレームの初期化
    df = pl.DataFrame()

    # project のリストを取得
    projects: list[Project] = Project.objects()
    project_count = len(projects)

    # データベースをクローズ
    client.close()

    # プロジェクト毎に並行処理
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        done_count = 0
        future_to_project = {
            executor.submit(worker, project): project for project in projects
        }
        for future in concurrent.futures.as_completed(future_to_project):
            # 進捗表示
            done_count += 1
            project = future_to_project[future]
            logger.info(f"{project.name} Done ({done_count}/{project_count})")

            # DataFrame に追加
            df = pl.concat([df, future.result()], how="diagonal")

    # DataFrame の null を 0 で埋める
    df = df.fill_null(0)

    # 進捗表示
    logger.info("Done.")

    # CSV に出力
    with open(args.output, "w") as f:
        df.write_csv(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--output", type=str, default="data/pull_request_basics.csv"
    )
    args = parser.parse_args()

    main(args)
