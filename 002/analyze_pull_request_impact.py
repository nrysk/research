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
    Project,
    PullRequest,
    PullRequestCommit,
    PullRequestSystem,
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
    vcs_system: VCSSystem = VCSSystem.objects(project_id=project.id).first()
    pull_request_system: PullRequestSystem = PullRequestSystem.objects(
        project_id=project.id
    ).first()

    commits: list[Commit] = Commit.objects(vcs_system_id=vcs_system.id).only("id")
    row = defaultdict(int)
    row["project"] = project.name
    row["#pr"] = PullRequest.objects(
        pull_request_system_id=pull_request_system.id
    ).count()
    row["#mpr"] = PullRequest.objects(
        pull_request_system_id=pull_request_system.id, merged_at__exists=True
    ).count()

    row |= dict(sorted(process_commits(commits, project).items()))

    return row


def process_commits(commits: list[Commit], project: Project) -> dict:
    row = defaultdict(int)
    commit_count = len(commits)
    for i, commit in enumerate(commits):
        # 進捗表示
        if i % 200 == 0:
            print(
                f"{datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")} {i / commit_count * 100:2.1f}% done. ({project.name})",
                file=sys.stderr,
            )

        # commit が pull_request に含まれているか判定
        pull_request_commit: list[PullRequestCommit] = (
            PullRequestCommit.objects(commit_id=commit.id).only().first()
        )
        in_pull_request = pull_request_commit is not None

        # commit が bug-inducing な変更を含んでいるかを判定
        file_actions: list[FileAction] = FileAction.objects(commit_id=commit.id).only(
            "induces.label"
        )
        is_bug_inducing_flags = defaultdict(bool)
        for file_action in file_actions:
            if not file_action.induces:
                continue
            for induce in file_action.induces:
                is_bug_inducing_flags[induce["label"].lower()] = True
        is_bug_inducing_flags["all"] = any(is_bug_inducing_flags.values())

        # プルリクエストの有無と bug-inducing の有無によって, commit をカウント
        szz_labels = [
            "all",
            "szz",
            "jl+r",
            "jlip+r",
            "jlmiv",
            "jlmiv+",
            "jlmiv+r",
            "jlmiv+av",
            "jlmiv+rav",
            "jlmivlv",
        ]
        for label in szz_labels:
            is_bug_inducing = is_bug_inducing_flags[label]
            if in_pull_request and is_bug_inducing:
                row[f"#cmt+pr+bi_{label}"] += 1
            elif in_pull_request and not is_bug_inducing:
                row[f"#cmt+pr-bi_{label}"] += 1
            elif not in_pull_request and is_bug_inducing:
                row[f"#cmt-pr+bi_{label}"] += 1
            elif not in_pull_request and not is_bug_inducing:
                row[f"#cmt-pr-bi_{label}"] += 1

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
        "-o", "--output", type=str, default="data/pull_request_impact.csv"
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
