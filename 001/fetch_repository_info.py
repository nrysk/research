import argparse
import os
import sys

import polars as pl
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from mongoengine import connect
from pycoshark.mongomodels import (
    Commit,
    Project,
    PullRequest,
    PullRequestSystem,
    VCSSystem,
)
from pycoshark.utils import create_mongodb_uri_string

GITHUB_GRAPHQL_ENDPOINT = "https://api.github.com/graphql"

BATCH_SIZE = 25


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

    # GraphQL クライアントの初期化
    transport = RequestsHTTPTransport(
        url=GITHUB_GRAPHQL_ENDPOINT,
        headers={"Authorization": f"Bearer {os.getenv('GITHUB_API_TOKEN')}"},
    )
    client = Client(transport=transport, fetch_schema_from_transport=True)

    # DataFrame の初期化
    df = pl.DataFrame()

    projects = Project.objects
    for i in range(0, len(projects), BATCH_SIZE):
        # 進捗表示
        print(f"{i/len(projects)*100:.2f}%", end="\r", file=sys.stderr)

        # プロジェクトのリポジトリ情報を取得するためのクエリを作成
        repository_query = ""
        for project in projects[i : i + BATCH_SIZE]:
            pull_request_system = PullRequestSystem.objects(project_id=project.id)
            if len(pull_request_system) > 1:
                print(f"Multiple pull request systems found for project {project.name}")
            pull_request_system = pull_request_system.first()
            owner, repository = pull_request_system.url.split("/")[-3:-1]

            repository_query += f"""
                project_{project.id}: repository(owner: "{owner}", name: "{repository}") {{
                    owner {{
                        login
                    }}
                    name
                    description
                    url
                    createdAt
                    archivedAt
                    autoMergeAllowed
                    assignableUsers {{
                        totalCount
                    }}
                    stargazers {{
                        totalCount
                    }}
                    mirrorUrl
                }},
            """

        # リポジトリ情報を取得
        query = gql(
            f"""
            {{
                {repository_query}
            }}
            """
        )
        result = client.execute(query)

        # リポジトリ情報を DataFrame に変換
        for key, value in result.items():
            row = {
                "project_id": key.split("_")[1],
            }
            for k, v in value.items():
                if isinstance(v, dict):
                    for kk, vv in v.items():
                        row[f"{k}.{kk}"] = vv if vv is not None else ""

                else:
                    row[k] = v if v is not None else ""

            # commit と pull request の数を取得
            vcs_system = VCSSystem.objects(project_id=row["project_id"]).first()
            pull_request_system = PullRequestSystem.objects(
                project_id=row["project_id"]
            ).first()
            row["#cmt"] = Commit.objects(vcs_system_id=vcs_system.id).count()
            row["#pr"] = PullRequest.objects(
                pull_request_system_id=pull_request_system.id
            ).count()
            row["#mpr"] = PullRequest.objects(
                pull_request_system_id=pull_request_system.id, merged_at__exists=True
            ).count()

            df = df.vstack(pl.DataFrame(row))

    # CSV に保存
    df.write_csv(args.output)

    # 進捗表示
    print("100.00%", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--output", help="Output file", default="data/repository_info.csv"
    )
    args = parser.parse_args()
    main(args)
