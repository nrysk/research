import argparse
import gc
import os
import sys
from collections import defaultdict

import polars as pl
from mongoengine import connect
from pycoshark.mongomodels import Commit, FileAction, Project, VCSSystem
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
        row = defaultdict(int)

        vcs_system: VCSSystem = VCSSystem.objects(project_id=project.id).first()
        row["project"] = project.name

        # commit のカウント
        row["nc"] = Commit.objects(vcs_system_id=vcs_system.id).count()

        # 進捗表示
        print(
            f"{i} projects done. Processing {row["nc"]} commits in {project.name}...",
            file=sys.stderr,
        )

        # commit の日付の最小値と最大値を取得
        row["fcd"] = (
            Commit.objects(vcs_system_id=vcs_system.id)
            .only("committer_date")
            .order_by("+committer_date")
            .first()
            .committer_date
        )
        row["lcd"] = (
            Commit.objects(vcs_system_id=vcs_system.id)
            .only("committer_date")
            .order_by("-committer_date")
            .first()
            .committer_date
        )

        # bug-fixing と bug-inducing のカウント
        commits: list[Commit] = Commit.objects(vcs_system_id=vcs_system.id).only(
            "id", "labels"
        )
        for j, commit in enumerate(commits):
            if j % 1000 == 0:
                # 進捗表示
                print(
                    f"{j / row["nc"] * 100:2.1f}% done.",
                    end="\r",
                    file=sys.stderr,
                )

            # bug-fixing のカウント
            is_bugfixing_a = commit.labels.get("adjustedszz_bugfix", False)
            is_bugfixing_io = commit.labels.get("issueonly_bugfix", False)
            is_bugfixing_v = commit.labels.get("validated_bugfix", False)
            is_bugfixing_if = commit.labels.get("issueonly_bugfix", False)
            is_bugfixing = (
                is_bugfixing_a or is_bugfixing_io or is_bugfixing_v or is_bugfixing_if
            )
            row["nbfc"] += is_bugfixing
            row["nbfc_a"] += is_bugfixing_a
            row["nbfc_io"] += is_bugfixing_io
            row["nbfc_v"] += is_bugfixing_v
            row["nbfc_if"] += is_bugfixing_if

            # bug-inducing のカウント
            flags = defaultdict(bool)
            file_actions: list[FileAction] = FileAction.objects(
                commit_id=commit.id
            ).only("induces")
            for file_action in file_actions:
                for induce in file_action.induces:
                    flags[induce["label"]] = True
            row["nbic"] += any(flags.values())
            for label, flag in flags.items():
                row[f"nbic_{label.lower()}"] += flag

        print(dict(row), file=sys.stderr)

        df = df.vstack(pl.DataFrame(row))

    print("Done.", file=sys.stderr)

    with open(args.output, "w") as f:
        df.write_csv(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", type=str, default="data/commit_info.csv")
    args = parser.parse_args()
    main(args)
