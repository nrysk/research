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
IGNORED_PROJECTS = (
    "jacketrabbit",
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
    rows = process_project(project)
    client.close()

    return pl.DataFrame(rows)


def process_project(project: Project) -> list[dict]:
    vcs_system_id = VCSSystem.objects(project_id=project.id).first().id
    pull_request_systems = PullRequestSystem.objects(project_id=project.id).only(
        "id", "url"
    )
    pull_request_system_ids = [
        pull_request_system.id for pull_request_system in pull_request_systems
    ]
    owner, repository = pull_request_systems[0].url.split("/")[-3:-1]
    pull_request_system_url = f"https://github.com/{owner}/{repository}/pull"

    # マージされた pull_request を取得
    pull_requests: list[PullRequest] = PullRequest.objects(
        pull_request_system_id__in=pull_request_system_ids, merged_at__exists=True
    ).only(
        "id",
        "external_id",
        "created_at",
        "merged_at",
        "source_repo_url",
        "target_repo_url",
        "creator_id",
    )

    rows = []
    for pull_request in pull_requests:
        # ========================================
        # 事前に必要なデータを取得
        # ========================================

        # プルリクエストコミット情報
        pull_request_commits: list[PullRequestCommit] = PullRequestCommit.objects(
            pull_request_id=pull_request.id,
            commit_id__exists=True,
        ).only("commit_id")
        # コミットが存在しない場合はスキップ
        if not pull_request_commits:
            continue
        commit_ids = [
            pull_request_commit.commit_id
            for pull_request_commit in pull_request_commits
        ]

        # コミット情報
        commits: list[Commit] = Commit.objects(id__in=commit_ids)

        # ファイルアクション情報
        file_actions: list[FileAction] = FileAction.objects(
            commit_id__in=commit_ids
        ).only("file_id", "induces")

        # ファイル情報
        file_ids = set(
            [file_action.file_id for file_action in file_actions if file_action.file_id]
        )
        files = File.objects(id__in=file_ids).only("path")

        # プルリクエストファイル情報
        pull_request_files: list[PullRequestFile] = PullRequestFile.objects(
            pull_request_id=pull_request.id
        ).only("path", "additions", "deletions")

        # プルリクエストコメント情報
        pull_request_comments: list[PullRequestComment] = (
            PullRequestComment.objects(pull_request_id=pull_request.id)
            .order_by("created_at")
            .only("created_at", "comment")
        )

        # プルリクエストレビュー情報
        pull_request_reviews: list[PullRequestReview] = (
            PullRequestReview.objects(pull_request_id=pull_request.id)
            .order_by("submitted_at")
            .only("submitted_at", "state")
        )

        # プルリクエストレビューコメント情報
        pull_request_review_ids = [
            pull_request_review.id for pull_request_review in pull_request_reviews
        ]
        pull_request_review_comments: list[PullRequestReviewComment] = (
            PullRequestReviewComment.objects(
                pull_request_review_id__in=pull_request_review_ids,
                comment__exists=True,
            )
            .order_by("created_at")
            .only("created_at")
        )

        # ========================================
        # レコードを作成
        # ========================================

        row = {}

        # プロジェクト名
        row["project"] = project.name

        # プルリクエストの識別子
        row["id"] = str(pull_request.id)

        # マージされるまでの時間 (分)
        row["age"] = (
            pull_request.merged_at - pull_request.created_at
        ).total_seconds() / 60

        # 含まれるコミット数
        row["#commits"] = len(pull_request_commits)

        # 追加行数・削除行数
        row["#added"] = 0
        row["#deleted"] = 0
        for pull_request_file in pull_request_files:
            row["#added"] += pull_request_file.additions
            row["#deleted"] += pull_request_file.deletions

        # 変更ファイル数
        row["#files"] = len(pull_request_files)

        # コメント数
        row["#comments"] = len(pull_request_comments)

        # レビューコメント数
        row["#review_comments"] = len(pull_request_review_comments)

        # Bot によるプルリクエストかどうか
        row["bot"] = str(pull_request.creator_id) in BOT_IDS

        # コード変更かどうか
        row["code_change"] = any(
            pull_request_file.path.endswith(tuple(SOURCE_FILE_EXTENSIONS))
            for pull_request_file in pull_request_files
        )

        # ソースとターゲットのリポジトリが同じかどうか
        source_owner, source_repository = (
            pull_request.source_repo_url.split("/")[-2:]
            if pull_request.source_repo_url
            else (None, None)
        )
        target_owner, target_repository = pull_request.target_repo_url.split("/")[-2:]
        row["same_repository"] = (
            source_owner == target_owner and source_repository == target_repository
        )

        # 最後のコメントにメンションが含まれているかどうか
        # TODO

        # 承認数・変更依頼数
        row["#approvals"] = 0
        row["#changes_requested"] = 0
        for pull_request_review in pull_request_reviews:
            if pull_request_review.state == "APPROVED":
                row["#approvals"] += 1
            if pull_request_review.state == "CHANGES_REQUESTED":
                row["#changes_requested"] += 1

        # 不具合修正かどうか
        row["fix"] = False
        for commit in commits:
            if "issueonly_bugfix" not in commit.labels:
                print(commit.id)
            if commit.labels["issueonly_bugfix"]:
                row["fix"] = True
                break

        # テストコードが含まれているかどうか
        row["test"] = False
        for pull_request_file in pull_request_files:
            if any(
                substring in pull_request_file.path.lower()
                for substring in ["test", "spec"]
            ):
                row["test"] = True

        # 不具合混入しているかどうか
        row["buggy"] = False
        for file_action in file_actions:
            for induce in file_action.induces:
                if induce["label"] == "JL+R":
                    row["buggy"] = True
                    break

        # プルリクエストへのリンク
        row["url"] = f"{pull_request_system_url}/{pull_request.external_id}"

        # ========================================
        # レコードを追加
        # ========================================
        rows.append(row)

    return rows


@timeit_decorator(logger=logger)
def main(args):
    # データベースに接続
    client = connect_to_mongodb()

    # データフレームの初期化
    df = pl.DataFrame()

    # プロジェクトの取得
    projects: list[Project] = Project.objects
    if args.small:
        projects = projects[:16]

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
    parser.add_argument(
        "-o", "--output", type=str, default="data/pull_request_features.csv"
    )
    parser.add_argument("--small", default=False, action="store_true")

    args = parser.parse_args()

    main(args)
