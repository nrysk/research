import argparse
import os
import sys
from collections import defaultdict

import polars as pl
from mongoengine import connect
from pycoshark.mongomodels import (
    Commit,
    FileAction,
    Hunk,
    Project,
    PullRequest,
    PullRequestComment,
    PullRequestReview,
    PullRequestReviewComment,
    PullRequestSystem,
    Refactoring,
    VCSSystem,
)
from pycoshark.utils import create_mongodb_uri_string

SZZ_LABELS = [
    "SZZ",
    "JL+R",
    "JLIP+R",
    "JLMIV",
    "JLMIV+",
    "JLMIV+AV",
    "JLMIV+RAV",
    "JLMIV+R",
]


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

    projects: list[Project] = Project.objects()
    for i, project in enumerate(projects):
        row = defaultdict(int)
        vcs_system: VCSSystem = VCSSystem.objects(project_id=project.id).first()
        pull_request_system: PullRequestSystem = PullRequestSystem.objects(
            project_id=project.id
        ).first()

        # プロジェクト名
        row["project"] = project.name

        # pull_request のカウント
        row["npr"] = PullRequest.objects(
            pull_request_system_id=pull_request_system.id
        ).count()

        # 進捗表示
        print(
            f"{i} projects done. Processing {row["npr"]} pull requests in {project.name}...",
            file=sys.stderr,
        )

        # マージ済み pull_request のカウント
        row["nmpr"] = PullRequest.objects(
            pull_request_system_id=pull_request_system.id,
            merged_at__exists=True,
        ).count()

        # リジェクトされた pull_request のカウント
        row["nrpr"] = PullRequest.objects(
            pull_request_system_id=pull_request_system.id,
            merged_at__exists=False,
            state="closed",
        ).count()

        # pull_request と commit_sha_list のペアを含む csv ファイルを読み込み
        repository = pull_request_system.url.split("/")[-2]
        lazy_df = pl.scan_csv("data/commits_on_pull_request.csv")
        filtered_df = (
            lazy_df.filter(pl.col("repository") == repository)
            .select("pull_request_id", "commit_sha_list")
            .with_columns(pl.col("commit_sha_list").str.split(","))
        ).collect()

        for pull_request_id, commit_sha_list in filtered_df.iter_rows():

            # commit_sha_list が list でない場合はスキップ
            if not isinstance(commit_sha_list, list):
                continue

            # commit を 1 つしか持たない pull_request のカウント
            row["nmpr_1c"] += len(commit_sha_list) == 1

            # commit を 30 以上持つ pull_request のカウント
            row["nmpr_30c"] += len(commit_sha_list) >= 30

            commits: list[Commit] = Commit.objects(
                vcs_system_id=vcs_system.id, revision_hash__in=commit_sha_list
            ).only("id", "labels")

            # データベース内に commit が存在するかのカウント
            row["nc_f"] += len(commits)
            row["nc_nf"] += len(commit_sha_list) - len(commits)
            row["npr_cf"] += len(commits) == len(commit_sha_list)
            row["npr_cnf"] += len(commits) == 0

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
            row["nbfpr"] += any(fixing_flags.values())
            row["nbfpr_a"] += fixing_flags["a"]
            row["nbfpr_io"] += fixing_flags["io"]
            row["nbfpr_v"] += fixing_flags["v"]
            row["nbfpr_if"] += fixing_flags["if"]

            # bug-inducing のカウント
            row["nbipr"] += any(inducing_flags.values())
            row["nbipr_szz"] += inducing_flags["SZZ"]
            row["nbipr_jl+r"] += inducing_flags["JL+R"]
            row["nbipr_jlip+r"] += inducing_flags["JLIP+R"]
            row["nbipr_jlmiv"] += inducing_flags["JLMIV"]
            row["nbipr_jlmiv+"] += inducing_flags["JLMIV+"]
            row["nbipr_jlmiv+av"] += inducing_flags["JLMIV+AV"]
            row["nbipr_jlmiv+rav"] += inducing_flags["JLMIV+RAV"]
            row["nbipr_jlmiv+r"] += inducing_flags["JLMIV+R"]

        df = pl.concat([df, pl.DataFrame(row)], how="diagonal")

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
    main(args)
