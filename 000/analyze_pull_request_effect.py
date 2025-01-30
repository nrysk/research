import argparse
import concurrent.futures
import os
import sys
from collections import defaultdict
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
    PullRequestCommit,
    PullRequestSystem,
    VCSSystem,
)
from pycoshark.utils import create_mongodb_uri_string

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.timeit_decorator import timeit_decorator

# ========================================
# 定数
# ========================================
IGNORED_PROJECTS = (
    "jackrabbit",
    "maven",
    "tapestry-5",
    "james",
    "commons-rdf",
    "bigtop",
)
BOT_IDS = ("5ff191c8c26a57681e7b99d0",)  # dependabot
SOURCE_FILE_EXTENSIONS = (
    # Java
    ".java",
    # Scala
    ".scala",
    # Python
    ".py",
    # C
    ".c",
    ".h",
    # C++
    ".cpp",
    ".hpp",
    ".cc",
    ".hh",
    ".cxx",
    ".hxx",
    # C#
    ".cs",
    # JavaScript
    ".js",
    # TypeScript
    ".ts",
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
    # 進捗表示
    if project.name in IGNORED_PROJECTS:
        logger.info(f"{project.name} Ignored")
        return pl.DataFrame()
    logger.info(f"{project.name} Start")

    client = connect_to_mongodb()
    row = process_project(project)
    client.close()
    return pl.DataFrame(row)


def process_project(project: Project) -> dict:
    row = {
        "project": project.name,
        "#cmt+pr+bi": 0,
        "#cmt+pr-bi": 0,
        "#cmt-pr+bi": 0,
        "#cmt-pr-bi": 0,
    }

    # プロジェクトに含まれるコミットを取得
    vcs_system: VCSSystem = VCSSystem.objects(project_id=project.id).first()
    commits: list[Commit] = Commit.objects(vcs_system_id=vcs_system.id).only("id")
    for i, commit in enumerate(commits):
        # 進捗表示
        if i % 500 == 0:
            logger.info(f"{project.name: <24} {i}/{len(commits)}")

        # コミットに含まれるファイルアクションとファイルを取得
        file_actions: list[FileAction] = FileAction.objects(
            commit_id=commit.id,
        ).only("induces.label", "file_id")
        file_ids = [file_action.file_id for file_action in file_actions]
        files = File.objects(id__in=file_ids).only("path")

        # コード変更を含まない場合はスキップ
        if not any(file.path.endswith(SOURCE_FILE_EXTENSIONS) for file in files):
            continue

        # commit が pull_request に含まれているか判定
        pull_request_commit: list[PullRequestCommit] = (
            PullRequestCommit.objects(commit_id=commit.id).only("author_id").first()
        )
        in_pull_request = False
        if pull_request_commit:
            # Bot によるコミットは除外
            if pull_request_commit.author_id in BOT_IDS:
                continue
            in_pull_request = True

        # コミットが不具合混入しているかを判定
        is_bug_inducing = False
        for file_action in file_actions:
            if not file_action.induces:
                continue
            for induce in file_action.induces:
                if induce["label"] == "JL+R":
                    is_bug_inducing = True
                    break

        # プルリクエストの有無と不具合混入の有無によって, コミットをカウント
        if in_pull_request and is_bug_inducing:
            row[f"#cmt+pr+bi"] += 1
        elif in_pull_request and not is_bug_inducing:
            row[f"#cmt+pr-bi"] += 1
        elif not in_pull_request and is_bug_inducing:
            row[f"#cmt-pr+bi"] += 1
        elif not in_pull_request and not is_bug_inducing:
            row[f"#cmt-pr-bi"] += 1

    return row


@timeit_decorator(logger=logger)
def main(args):
    # データベースに接続
    client = connect_to_mongodb()

    # DataFrame の初期化
    df = pl.DataFrame()

    # project のリストを取得
    projects: list[Project] = Project.objects
    if args.small:
        projects = projects[:16]
    project_count = len(projects)

    # データベースをクローズ
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
            logger.info(f"{project.name} Done ({done_count}/{project_count})")
            # 結果を DataFrame に追加
            df = pl.concat([df, future.result()], how="diagonal")

    # null を 0 で埋める
    df = df.fill_null(0)

    # CSV に出力
    with open(args.output, "w") as f:
        df.write_csv(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--output", type=str, default="data/pull_request_effect.csv"
    )
    parser.add_argument("--small", default=False, action="store_true")
    args = parser.parse_args()

    main(args)
