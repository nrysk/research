import argparse
import concurrent.futures
import os
import sys
from collections import defaultdict
from datetime import datetime

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
    client = connect_to_mongodb()
    row = process_project(project)
    client.close()
    return pl.DataFrame(row)


def process_project(project: Project) -> dict:
    row = defaultdict(int)

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

    # pull_request の日付の最小値と最大値を取得
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
    row["fprd"] = first_pull_request.created_at if first_pull_request else None
    row["lprd"] = last_pull_request.created_at if last_pull_request else None

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

        # commit_ids の欠損度合いをカウント
        row["nmpr_all"] += all(commit_ids)
        row["nmpr_partial"] += any(commit_ids) and not all(commit_ids)
        row["nmpr_none"] += not any(commit_ids)

        # commit_ids が欠損している場合はスキップ
        if None in commit_ids:
            continue

        # commit のリストを取得
        commits: list[Commit] = Commit.objects(id__in=commit_ids).only("id", "labels")

        # commit 数の度数分布を取得
        row["nmpr_nc==0"] += len(commits) == 0
        row["nmpr_nc==1"] += len(commits) == 1
        row["nmpr_1<nc<=5"] += 1 <= len(commits) <= 5
        row["nmpr_5<nc<=10"] += 5 < len(commits) <= 10
        row["nmpr_10<nc<=20"] += 10 < len(commits) <= 20
        row["nmpr_20<nc<=30"] += 20 < len(commits) <= 30
        row["nmpr_30<nc"] += 30 < len(commits)

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
    projects: list[Project] = Project.objects()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        done_count = 0
        future_to_project = {
            executor.submit(worker, project): project for project in projects
        }
        for future in concurrent.futures.as_completed(future_to_project):
            # 進捗表示
            done_count += 1
            project = future_to_project[future]
            print(
                f"{datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')} [{done_count:02}/{project_count:02}] {project.name} done.",
                file=sys.stderr,
            )

            # DataFrame に追加
            df = pl.concat([df, future.result()], how="diagonal")

    # DataFrame の null を 0 で埋める
    df = df.fill_null(0)

    # 進捗表示
    print("Done.", file=sys.stderr)

    # CSV に出力
    with open(args.output, "w") as f:
        df.write_csv(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--output", type=str, default="data/pull_request_info.csv"
    )
    args = parser.parse_args()
    # 処理開始時刻を記録
    start_time = datetime.now()
    print(
        f"{start_time.strftime('[%Y-%m-%d %H:%M:%S]')} Start processing.",
        file=sys.stderr,
    )

    main(args)

    # 処理終了時刻を記録
    end_time = datetime.now()
    print(f"{end_time.strftime('[%Y-%m-%d %H:%M:%S]')} Done.", file=sys.stderr)
    # 処理時間を出力
    print(f"Elapsed time: {end_time - start_time}", file=sys.stderr)
