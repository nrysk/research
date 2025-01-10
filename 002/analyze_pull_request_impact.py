import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime

import polars as pl
from mongoengine import connect
from pycoshark.mongomodels import (
    Commit,
    FileAction,
    Project,
    PullRequest,
    PullRequestCommit,
    PullRequestSystem,
    VCSSystem,
)
from pycoshark.utils import create_mongodb_uri_string


def main(args):
    # データベースに接続
    uri = create_mongodb_uri_string(
        db_user=os.getenv("SMARTSHARK_DB_USERNAME"),
        db_password=os.getenv("SMARTSHARK_DB_PASSWORD"),
        db_hostname=os.getenv("SMARTSHARK_DB_HOST"),
        db_port=os.getenv("SMARTSHARK_DB_PORT"),
        db_authentication_database=os.getenv("SMARTSHARK_DB_AUTHENTICATION_DATABASE"),
        db_ssl_enabled=False,
    )
    connect(
        db=os.getenv("SMARTSHARK_DB_DATABASE"),
        host=uri,
    )

    # DataFrame の初期化
    df = pl.DataFrame()

    # project 毎に処理
    projects: list[Project] = Project.objects()
    for i, project in enumerate(projects):
        vcs_system: VCSSystem = VCSSystem.objects(project_id=project.id).first()
        pull_request_system = PullRequestSystem.objects(project_id=project.id).first()
        commit_count = Commit.objects(vcs_system_id=vcs_system.id).count()

        # 進捗表示
        print(
            f"{datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")} {i} projects done. Processing {commit_count} commits in {project.name}...",
            file=sys.stderr,
        )

        commits: list[Commit] = Commit.objects(vcs_system_id=vcs_system.id).only("id")
        row = defaultdict(int)
        row["project"] = project.name
        row["#pr"] = PullRequest.objects(
            pull_request_system_id=pull_request_system.id
        ).count()
        row["#mpr"] = PullRequest.objects(
            pull_request_system_id=pull_request_system.id, merged_at__exists=True
        ).count()
        for j, commit in enumerate(commits):
            # 進捗表示
            if j % 100 == 0:
                print(
                    f"{datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")} {j / commit_count * 100:2.1f}% done.",
                    end="\r",
                    file=sys.stderr,
                )

            # commit が pull_request に含まれているか判定
            pull_request_commit: list[PullRequestCommit] = (
                PullRequestCommit.objects(commit_id=commit.id).only().first()
            )
            in_pull_request = pull_request_commit is not None

            # commit が bug-inducing な変更を含んでいるかを判定
            file_actions: list[FileAction] = FileAction.objects(
                commit_id=commit.id
            ).only("induces")
            is_bug_inducing = any(file_action.induces for file_action in file_actions)

            if in_pull_request and is_bug_inducing:
                row["#+pr+bi"] += 1
            elif in_pull_request and not is_bug_inducing:
                row["#+pr-bi"] += 1
            elif not in_pull_request and is_bug_inducing:
                row["#-pr+bi"] += 1
            elif not in_pull_request and not is_bug_inducing:
                row["#-pr-bi"] += 1

        # DataFrame に追加
        df = pl.concat([df, pl.DataFrame(row)], how="diagonal")

    print(f"{datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")} Done.", file=sys.stderr)

    # null を 0 で埋める
    df = df.fill_nulls(0)

    # CSV に出力
    with open(args.output, "w") as f:
        df.write_csv(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--output", type=str, default="data/pull_request_impact.csv"
    )
    args = parser.parse_args()
    main(args)
