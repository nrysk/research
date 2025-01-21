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
    Hunk,
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

    # pull_request_system の id のリストを取得
    pull_request_system: PullRequestSystem = PullRequestSystem.objects(
        project_id=project.id
    )
    pull_request_system_ids = [prs.id for prs in pull_request_system]

    # プロジェクト名
    row["project"] = project.name

    # pull_request のカウント
    row["npr"] = PullRequest.objects(
        pull_request_system_id__in=pull_request_system_ids
    ).count()

    # マージされた pull_request のカウント
    row["nmpr"] = PullRequest.objects(
        pull_request_system_id__in=pull_request_system_ids,
        merged_at__exists=True,
    ).count()

    # リジェクトされた pull_request のカウント
    row["nrpr"] = PullRequest.objects(
        pull_request_system_id__in=pull_request_system_ids,
        merged_at__exists=False,
        state="closed",
    ).count()

    # マージされた pull_request 毎に commit の情報を取得
    pull_requests: list[PullRequest] = PullRequest.objects(
        pull_request_system_id__in=pull_request_system_ids,
        merged_at__exists=True,
    ).only("id")
    for pull_request in pull_requests:
        # pull_request に紐づいた commit_id のリストを取得
        pull_request_commits: list[PullRequestCommit] = PullRequestCommit.objects(
            pull_request_id=pull_request.id
        ).only("commit_id")
        commit_ids = [pc.commit_id for pc in pull_request_commits]

        # commit_ids が欠損している場合はスキップ
        if None in commit_ids:
            continue

        # commit のリストを取得
        commits: list[Commit] = Commit.objects(id__in=commit_ids).only("id", "labels")

        # bug-fixing と bug-inducing のカウント
        fixing_flags = defaultdict(bool)
        inducing_flags = defaultdict(bool)
        for commit in commits:
            # bug-fixing か判定
            fixing_flags["a"] |= commit.labels.get("adjustedszz_bugfix", False)
            fixing_flags["io"] |= commit.labels.get("issueonly_bugfix", False)
            fixing_flags["v"] |= commit.labels.get("validated_bugfix", False)
            fixing_flags["if"] |= commit.labels.get("issueonly_bugfix", False)

            # bug-inducing か判定
            file_actions: list[FileAction] = FileAction.objects(
                commit_id=commit.id
            ).only("induces")
            for file_action in file_actions:
                for induce in file_action.induces:
                    inducing_flags[induce["label"]] = True

        # bug-fixing のカウント
        row["nmbfpr"] += any(fixing_flags.values())
        for label, flag in fixing_flags.items():
            row[f"nmbfpr_{label}"] += flag

        # bug-inducing のカウント
        row["nmbipr"] += any(inducing_flags.values())
        for label, flag in inducing_flags.items():
            row[f"nmbipr_{label.lower()}"] += flag

    return row


@timeit_decorator(logger=logger)
def main(args):
    # データベースに接続
    client = connect_to_mongodb()

    # DataFrame の初期化
    df = pl.DataFrame()

    # project のリストを取得
    projects: list[Project] = Project.objects()
    project_count = len(projects)

    # データベースをクローズ
    client.close()

    # project を並行処理
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
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
        "-o", "--output", type=str, default="data/pull_request_defects.csv"
    )
    args = parser.parse_args()

    main(args)
