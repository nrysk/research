import argparse
import csv
import os
import sys

import requests
from mongoengine import connect
from pycoshark.mongomodels import (
    Commit,
    Project,
    PullRequest,
    PullRequestSystem,
    VCSSystem,
)
from pycoshark.utils import create_mongodb_uri_string

BATCH_SIZE = 100


def fetch_commits_on_pull_requests(
    owner: str,
    repository: str,
    pull_request_numbers: list[int],
    token: str,
) -> dict[str, list[str]]:
    url = f"https://api.github.com/graphql"
    headers = {"Authorization": f"token {token}", "Content-Type": "application/json"}

    # BATCH_SIZE 毎に query を作成し, queries に追加
    queries = []
    for i in range(0, len(pull_request_numbers), BATCH_SIZE):
        inner_queries = [
            f"""
            pull_request_{pull_request_number}: pullRequest(number: {pull_request_number}) {{
                commits(first: 30) {{
                    nodes {{
                        commit {{
                            oid
                        }}
                    }}
                }}
            }}
            """
            for pull_request_number in pull_request_numbers[i : i + BATCH_SIZE]
        ]
        queries.append(
            f"""
            query {{
                repository(owner: "{owner}", name: "{repository}") {{
                    {' '.join(inner_queries)}
                }}
            }}
            """
        )

    # query 毎にリクエストを送信し, pull_request_to_commits に追加
    pull_request_to_commits: dict[str, list[str]] = {}

    for query in queries:
        res = requests.post(url, headers=headers, json={"query": query})
        res.raise_for_status()
        data = res.json()
        for key, value in data["data"]["repository"].items():
            number = key.removeprefix("pull_request_")
            if value is None:
                pull_request_to_commits[number] = ["Not Found"]
            else:
                pull_request_to_commits[number] = [
                    node["commit"]["oid"] for node in value["commits"]["nodes"]
                ]

    return pull_request_to_commits


def main(args):
    # GitHub API トークンの取得
    token = os.getenv("GITHUB_API_TOKEN")

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

    # 出力ファイルの準備
    with open(args.output, "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "owner",
                "repository",
                "pull_request_id",
                "pull_request_number",
                "commit_sha_list",
            ]
        )

    # プロジェクト毎に処理
    projects: list[Project] = Project.objects()
    for i, project in enumerate(projects):
        pull_request_system: PullRequestSystem = PullRequestSystem.objects(
            project_id=project.id
        ).first()
        owner = pull_request_system.url.split("/")[-3]
        repository = pull_request_system.url.split("/")[-2]

        # マージ済みプルリクエストの情報を取得
        pull_requests: list[PullRequest] = PullRequest.objects(
            pull_request_system_id=pull_request_system.id,
            merged_at__exists=True,
        ).only("id", "external_id")

        # 進捗表示
        print(
            f"{i} projects done. Processing {pull_requests.count()} pull requests in {owner}/{repository}...",
            file=sys.stderr,
        )

        # プルリクエスト毎にコミット情報を取得
        pull_request_to_commits = fetch_commits_on_pull_requests(
            owner=owner,
            repository=repository,
            pull_request_numbers=[pr.external_id for pr in pull_requests],
            token=token,
        )

        # CSV に出力
        with open(args.output, "a") as f:
            writer = csv.writer(f)
            for pull_request in pull_requests:
                writer.writerow(
                    [
                        owner,
                        repository,
                        pull_request.id,
                        pull_request.external_id,
                        ",".join(
                            pull_request_to_commits.get(
                                pull_request.external_id, ["Not Found"]
                            )
                        ),
                    ]
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--output", type=str, default="commits_on_pull_request.csv"
    )
    args = parser.parse_args()
    main(args)
