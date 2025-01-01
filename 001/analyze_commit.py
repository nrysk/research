import argparse
import gc
import os
import sys
from collections import defaultdict

import polars as pl
from mongoengine import connect
from pycoshark.mongomodels import Commit, FileAction, Project, VCSSystem
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

    # project 毎に処理
    projects: list[Project] = Project.objects()
    for i, project in enumerate(projects):
        row = defaultdict(int)

        vcs_system: VCSSystem = VCSSystem.objects(project_id=project.id).first()
        row["project"] = project.name
        row["nc"] = Commit.objects(vcs_system_id=vcs_system.id).count()

        # 進捗表示
        print(
            f"{i} projects done. Processing {row["nc"]} commits in {project.name}...",
            file=sys.stderr,
        )

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

                # メモリ解放
                gc.collect()

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
            flags = {label: False for label in SZZ_LABELS}
            file_actions: list[FileAction] = FileAction.objects(
                commit_id=commit.id
            ).only("induces")
            for file_action in file_actions:
                for induce in file_action.induces:
                    flags[induce["label"]] = True
            row["nbic"] += any(flags.values())
            row["nbic_szz"] += flags["SZZ"]
            row["nbic_jl+r"] += flags["JL+R"]
            row["nbic_jlip+r"] += flags["JLIP+R"]
            row["nbic_jlmiv"] += flags["JLMIV"]
            row["nbic_jlmiv+"] += flags["JLMIV+"]
            row["nbic_jlmiv+av"] += flags["JLMIV+AV"]
            row["nbic_jlmiv+rav"] += flags["JLMIV+RAV"]
            row["nbic_jlmiv+r"] += flags["JLMIV+R"]

            del commit, file_actions, flags

        print(row.__str__(), file=sys.stderr)

        df = df.vstack(pl.DataFrame(row))

    with open(args.output, "w") as f:
        df.write_csv(f)

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", type=str, default="commit_info.csv")
    args = parser.parse_args()
    main(args)
