from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.functions import col, upper, count

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("ReadJsonWithSpark") \
    .master("local[*]") \
    .getOrCreate()

# Kiểm tra SparkSession
print("SparkSession đã được khởi tạo thành công!")

# Đường dẫn đến file
file_path = r"E:\Learn_Data_Engineer\Data\2015_03_17_dta.json"

schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ]), True),
    StructField("repo", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True)
    ]), True),
    StructField("payload", StructType([
        StructField("action", StringType(), True),
        StructField("issue", StructType([
            StructField("url", StringType(), True),
            StructField("labels_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("number", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("user", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True)
            ]), True),
            StructField("labels", ArrayType(
            StructType([
                StructField("url", StringType(), True),
                StructField("name", StringType(), True),
                StructField("color", StringType(), True)
            ])
            ), True),
            StructField("state", StringType(), True),
            StructField("locked", BooleanType(), True),
            StructField("assignee", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True)
            ]), True),
            StructField("milestone", StructType([
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("labels_url", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("number", IntegerType(), True),
                StructField("title", StringType(), True),
                StructField("description", StringType(), True),
                StructField("creator", StructType([
                    StructField("login", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("avatar_url", StringType(), True),
                    StructField("gravatar_id", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("html_url", StringType(), True),
                    StructField("followers_url", StringType(), True),
                    StructField("following_url", StringType(), True),
                    StructField("gists_url", StringType(), True),
                    StructField("starred_url", StringType(), True),
                    StructField("subscriptions_url", StringType(), True),
                    StructField("organizations_url", StringType(), True),
                    StructField("repos_url", StringType(), True),
                    StructField("events_url", StringType(), True),
                    StructField("received_events_url", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("site_admin", BooleanType(), True)
                ]), True),
                StructField("open_issues", IntegerType(), True),
                StructField("closed_issues", IntegerType(), True),
                StructField("state", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
                StructField("due_on", StringType(), True),
                StructField("closed_at", StringType(), True)
            ]), True),
            StructField("comments", IntegerType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("closed_at", StringType(), True),
            StructField("body", StringType(), True),
            StructField("pull_request", StructType([
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("diff_url", StringType(), True),
                StructField("patch_url", StringType(), True)
            ]), True)
        ]), True),
        StructField("forkee", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("owner", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True)
            ]), True),
            StructField("private", BooleanType(), True),
            StructField("html_url", StringType(), True),
            StructField("description", StringType(), True),
            StructField("fork", BooleanType(), True),
            StructField("url", StringType(), True),
            StructField("forks_url", StringType(), True),
            StructField("keys_url", StringType(), True),
            StructField("collaborators_url", StringType(), True),
            StructField("teams_url", StringType(), True),
            StructField("hooks_url", StringType(), True),
            StructField("issue_events_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("assignees_url", StringType(), True),
            StructField("branches_url", StringType(), True),
            StructField("tags_url", StringType(), True),
            StructField("blobs_url", StringType(), True),
            StructField("git_tags_url", StringType(), True),
            StructField("git_refs_url", StringType(), True),
            StructField("trees_url", StringType(), True),
            StructField("statuses_url", StringType(), True),
            StructField("languages_url", StringType(), True),
            StructField("stargazers_url", StringType(), True),
            StructField("contributors_url", StringType(), True),
            StructField("subscribers_url", StringType(), True),
            StructField("subscription_url", StringType(), True),
            StructField("commits_url", StringType(), True),
            StructField("git_commits_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("issue_comment_url", StringType(), True),
            StructField("contents_url", StringType(), True),
            StructField("compare_url", StringType(), True),
            StructField("merges_url", StringType(), True),
            StructField("archive_url", StringType(), True),
            StructField("downloads_url", StringType(), True),
            StructField("issues_url", StringType(), True),
            StructField("pulls_url", StringType(), True),
            StructField("milestones_url", StringType(), True),
            StructField("notifications_url", StringType(), True),
            StructField("labels_url", StringType(), True),
            StructField("releases_url", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("pushed_at", StringType(), True),
            StructField("git_url", StringType(), True),
            StructField("ssh_url", StringType(), True),
            StructField("clone_url", StringType(), True),
            StructField("svn_url", StringType(), True),
            StructField("homepage", StringType(), True),
            StructField("size", IntegerType(), True),
            StructField("stargazers_count", IntegerType(), True),
            StructField("watchers_count", IntegerType(), True),
            StructField("language", StringType(), True),
            StructField("has_issues", BooleanType(), True),
            StructField("has_downloads", BooleanType(), True),
            StructField("has_wiki", BooleanType(), True),
            StructField("has_pages", BooleanType(), True),
            StructField("forks_count", IntegerType(), True),
            StructField("mirror_url", StringType(), True),
            StructField("open_issues_count", IntegerType(), True),
            StructField("forks", IntegerType(), True),
            StructField("open_issues", IntegerType(), True),
            StructField("watchers", IntegerType(), True),
            StructField("default_branch", StringType(), True),
            StructField("public", BooleanType(), True)
        ]), True),
        StructField("comment", StructType([
            StructField("url", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("diff_hunk", StringType(), True),
            StructField("path", StringType(), True),
            StructField("position", StringType(), True),
            StructField("original_position", IntegerType(), True),
            StructField("commit_id", StringType(), True),
            StructField("original_commit_id", StringType(), True),
            StructField("user", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True)
            ]), True),
            StructField("body", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("pull_request_url", StringType(), True),
            StructField("_links", StructType([
                StructField("self", StructType([
                    StructField("href", StringType(), True)
                ]), True),
                StructField("html", StructType([
                    StructField("href", StringType(), True)
                ]), True),
                StructField("pull_request", StructType([
                    StructField("href", StringType(), True)
                ]), True)
            ]), True),
            StructField("issue_url", StringType(), True),
            StructField("line", StringType(), True)
        ]), True),
        StructField("pull_request", StructType([
            StructField("url", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("html_url", StringType(), True),
            StructField("diff_url", StringType(), True),
            StructField("patch_url", StringType(), True),
            StructField("issue_url", StringType(), True),
            StructField("number", IntegerType(), True),
            StructField("state", StringType(), True),
            StructField("locked", BooleanType(), True),
            StructField("title", StringType(), True),
            StructField("user", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True)
            ]), True),
            StructField("body", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("closed_at", StringType(), True),
            StructField("merged_at", StringType(), True),
            StructField("merge_commit_sha", StringType(), True),
            StructField("assignee", StringType(), True),
            StructField("milestone", StringType(), True),
            StructField("commits_url", StringType(), True),
            StructField("review_comments_url", StringType(), True),
            StructField("review_comment_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("statuses_url", StringType(), True),
            StructField("head", StructType([
                StructField("label", StringType(), True),
                StructField("ref", StringType(), True),
                StructField("sha", StringType(), True),
                StructField("user", StructType([
                    StructField("login", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("avatar_url", StringType(), True),
                    StructField("gravatar_id", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("html_url", StringType(), True),
                    StructField("followers_url", StringType(), True),
                    StructField("following_url", StringType(), True),
                    StructField("gists_url", StringType(), True),
                    StructField("starred_url", StringType(), True),
                    StructField("subscriptions_url", StringType(), True),
                    StructField("organizations_url", StringType(), True),
                    StructField("repos_url", StringType(), True),
                    StructField("events_url", StringType(), True),
                    StructField("received_events_url", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("site_admin", BooleanType(), True)
                ]), True),
                StructField("repo", StringType(), True)
            ]), True),
            StructField("base", StructType([
                StructField("label", StringType(), True),
                StructField("ref", StringType(), True),
                StructField("sha", StringType(), True),
                StructField("user", StructType([
                    StructField("login", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("avatar_url", StringType(), True),
                    StructField("gravatar_id", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("html_url", StringType(), True),
                    StructField("followers_url", StringType(), True),
                    StructField("following_url", StringType(), True),
                    StructField("gists_url", StringType(), True),
                    StructField("starred_url", StringType(), True),
                    StructField("subscriptions_url", StringType(), True),
                    StructField("organizations_url", StringType(), True),
                    StructField("repos_url", StringType(), True),
                    StructField("events_url", StringType(), True),
                    StructField("received_events_url", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("site_admin", BooleanType(), True)
                ]), True),
                StructField("repo", StructType([
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("full_name", StringType(), True),
                    StructField("owner", StructType([
                        StructField("login", StringType(), True),
                        StructField("id", IntegerType(), True),
                        StructField("avatar_url", StringType(), True),
                        StructField("gravatar_id", StringType(), True),
                        StructField("url", StringType(), True),
                        StructField("html_url", StringType(), True),
                        StructField("followers_url", StringType(), True),
                        StructField("following_url", StringType(), True),
                        StructField("gists_url", StringType(), True),
                        StructField("starred_url", StringType(), True),
                        StructField("subscriptions_url", StringType(), True),
                        StructField("organizations_url", StringType(), True),
                        StructField("repos_url", StringType(), True),
                        StructField("events_url", StringType(), True),
                        StructField("received_events_url", StringType(), True),
                        StructField("type", StringType(), True),
                        StructField("site_admin", BooleanType(), True)
                    ]), True),
                    StructField("private", BooleanType(), True),
                    StructField("html_url", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("fork", BooleanType(), True),
                    StructField("url", StringType(), True),
                    StructField("forks_url", StringType(), True),
                    StructField("keys_url", StringType(), True),
                    StructField("collaborators_url", StringType(), True),
                    StructField("teams_url", StringType(), True),
                    StructField("hooks_url", StringType(), True),
                    StructField("issue_events_url", StringType(), True),
                    StructField("events_url", StringType(), True),
                    StructField("assignees_url", StringType(), True),
                    StructField("branches_url", StringType(), True),
                    StructField("tags_url", StringType(), True),
                    StructField("blobs_url", StringType(), True),
                    StructField("git_tags_url", StringType(), True),
                    StructField("git_refs_url", StringType(), True),
                    StructField("trees_url", StringType(), True),
                    StructField("statuses_url", StringType(), True),
                    StructField("languages_url", StringType(), True),
                    StructField("stargazers_url", StringType(), True),
                    StructField("contributors_url", StringType(), True),
                    StructField("subscribers_url", StringType(), True),
                    StructField("subscription_url", StringType(), True),
                    StructField("commits_url", StringType(), True),
                    StructField("git_commits_url", StringType(), True),
                    StructField("comments_url", StringType(), True),
                    StructField("issue_comment_url", StringType(), True),
                    StructField("contents_url", StringType(), True),
                    StructField("compare_url", StringType(), True),
                    StructField("merges_url", StringType(), True),
                    StructField("archive_url", StringType(), True),
                    StructField("downloads_url", StringType(), True),
                    StructField("issues_url", StringType(), True),
                    StructField("pulls_url", StringType(), True),
                    StructField("milestones_url", StringType(), True),
                    StructField("notifications_url", StringType(), True),
                    StructField("labels_url", StringType(), True),
                    StructField("releases_url", StringType(), True),
                    StructField("created_at", StringType(), True),
                    StructField("updated_at", StringType(), True),
                    StructField("pushed_at", StringType(), True),
                    StructField("git_url", StringType(), True),
                    StructField("ssh_url", StringType(), True),
                    StructField("clone_url", StringType(), True),
                    StructField("svn_url", StringType(), True),
                    StructField("homepage", StringType(), True),
                    StructField("size", IntegerType(), True),
                    StructField("stargazers_count", IntegerType(), True),
                    StructField("watchers_count", IntegerType(), True),
                    StructField("language", StringType(), True),
                    StructField("has_issues", BooleanType(), True),
                    StructField("has_downloads", BooleanType(), True),
                    StructField("has_wiki", BooleanType(), True),
                    StructField("has_pages", BooleanType(), True),
                    StructField("forks_count", IntegerType(), True),
                    StructField("mirror_url", StringType(), True),
                    StructField("open_issues_count", IntegerType(), True),
                    StructField("forks", IntegerType(), True),
                    StructField("open_issues", IntegerType(), True),
                    StructField("watchers", IntegerType(), True),
                    StructField("default_branch", StringType(), True)
                ]), True)
            ]), True),
            StructField("_links", StructType([
                StructField("self", StructType([
                    StructField("href", StringType(), True)
                ]), True),
                StructField("html", StructType([
                    StructField("href", StringType(), True)
                ]), True),
                StructField("issue", StructType([
                    StructField("href", StringType(), True)
                ]), True),
                StructField("comments", StructType([
                    StructField("href", StringType(), True)
                ]), True),
                StructField("review_comments", StructType([
                    StructField("href", StringType(), True)
                ]), True),
                StructField("review_comment", StructType([
                    StructField("href", StringType(), True)
                ]), True),
                StructField("commits", StructType([
                    StructField("href", StringType(), True)
                ]), True),
                StructField("statuses", StructType([
                    StructField("href", StringType(), True)
                ]), True)
            ]), True),
            StructField("merged", BooleanType(), True),
            StructField("mergeable", StringType(), True),
            StructField("mergeable_state", StringType(), True),
            StructField("merged_by", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True)
            ]), True),
            StructField("comments", IntegerType(), True),
            StructField("review_comments", IntegerType(), True),
            StructField("commits", IntegerType(), True),
            StructField("additions", IntegerType(), True),
            StructField("deletions", IntegerType(), True),
            StructField("changed_files", IntegerType(), True)
        ]), True),
        StructField("push_id", IntegerType(), True),
        StructField("size", IntegerType(), True),
        StructField("distinct_size", IntegerType(), True),
        StructField("ref", StringType(), True),
        StructField("head", StringType(), True),
        StructField("before", StringType(), True),
        StructField("commits", ArrayType(
        StructType([
            StructField("sha", StringType(), True),
            StructField("author",             StructType([
                StructField("email", StringType(), True),
                StructField("name", StringType(), True)
            ]), True),
            StructField("message", StringType(), True),
            StructField("distinct", StringType(), True),
            StructField("url", StringType(), True)
        ])
        ), True),
        StructField("ref_type", StringType(), True),
        StructField("master_branch", StringType(), True),
        StructField("description", StringType(), True),
        StructField("pusher_type", StringType(), True),
        StructField("member", StructType([
            StructField("login", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("avatar_url", StringType(), True),
            StructField("gravatar_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("followers_url", StringType(), True),
            StructField("following_url", StringType(), True),
            StructField("gists_url", StringType(), True),
            StructField("starred_url", StringType(), True),
            StructField("subscriptions_url", StringType(), True),
            StructField("organizations_url", StringType(), True),
            StructField("repos_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("received_events_url", StringType(), True),
            StructField("type", StringType(), True),
            StructField("site_admin", BooleanType(), True)
        ]), True),
        StructField("number", IntegerType(), True),
        StructField("release", StructType([
            StructField("url", StringType(), True),
            StructField("assets_url", StringType(), True),
            StructField("upload_url", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("tag_name", StringType(), True),
            StructField("target_commitish", StringType(), True),
            StructField("name", StringType(), True),
            StructField("draft", BooleanType(), True),
            StructField("author", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True)
            ]), True),
            StructField("prerelease", BooleanType(), True),
            StructField("created_at", StringType(), True),
            StructField("published_at", StringType(), True),
            StructField("assets", StructType([
                StructField("url", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("label", StringType(), True),
                StructField("uploader", StructType([
                    StructField("login", StringType(), True),
                    StructField("id", IntegerType(), True),
                    StructField("avatar_url", StringType(), True),
                    StructField("gravatar_id", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("html_url", StringType(), True),
                    StructField("followers_url", StringType(), True),
                    StructField("following_url", StringType(), True),
                    StructField("gists_url", StringType(), True),
                    StructField("starred_url", StringType(), True),
                    StructField("subscriptions_url", StringType(), True),
                    StructField("organizations_url", StringType(), True),
                    StructField("repos_url", StringType(), True),
                    StructField("events_url", StringType(), True),
                    StructField("received_events_url", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("site_admin", BooleanType(), True)
                ]), True),
                StructField("content_type", StringType(), True),
                StructField("state", StringType(), True),
                StructField("size", IntegerType(), True),
                StructField("download_count", IntegerType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
                StructField("browser_download_url", StringType(), True)
            ]), True),
            StructField("tarball_url", StringType(), True),
            StructField("zipball_url", StringType(), True),
            StructField("body", StringType(), True)
        ]), True),
        StructField("pages", StructType([
            StructField("page_name", StringType(), True),
            StructField("title", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("action", StringType(), True),
            StructField("sha", StringType(), True),
            StructField("html_url", StringType(), True)
        ]), True)
    ]), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("org", StructType([
        StructField("id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ]), True)
])
# Đọc file JSON với schema đã xây dựng
df = spark.read.schema(schema).json("2015_03_17_dta.json")

# Kiểm tra kết quả
#df.printSchema()
#df.show(5, truncate=False)
# df.select(df["payload.issue.comments_url"].alias("comments_url"),
#           df["payload.forkee.owner.login"].alias("login"),
#           df["payload.forkee.owner.id"].alias("id")).show()

# df.select(
#     col("id")*2,
#     upper(col("type")).alias("upper_type")
# ).show()
# from pyspark.sql.functions import col, countDistinct
# df.select(col("payload.forkee.owner.type").alias("in_type")) \
#         .distinct() \
#         .select(countDistinct("in_type").alias("count_type")) \
#         .show()
#
# df.select(col("payload.forkee.owner.type").alias("in_type")) \
#         .distinct() \
#         .select(count("*").alias("count_type")) \
#         .show()

# df.select(col("payload.forkee.owner.type").alias("in_type")) \
#   .groupBy("in_type") \
#   .count() \
#   .show()

# df.select(col("payload.forkee.owner.type").alias("in_type")) \
#         .groupBy("in_type") \
#         .count().alias("count_in_group") \
#         .show()

# Sort

df.select(col("payload.forkee.owner.type").alias("in_type"), col("payload.forkee.owner.id").alias("in_id")) \
        .where(col("in_type").isNotNull()) \
        .orderBy(col("in_type").desc(), col("in_id").asc()) \
        .show()