"""Social ETL script."""

import sqlite3

import praw

REDDIT_CLIENT_ID = "replace-with-your-reddit-client-id"
REDDIT_CLIENT_SECRET = "replace-with-your-reddit-client-secret"
REDDIT_USER_AGENT = "replace-with-your-reddit-user-agent"


def extract():
    """Function to extract data from Reddit, specifically from the subreddit
    'dataengineering'"""

    client = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )
    subreddit = client.subreddit("dataengineering")
    top_subreddit = subreddit.hot(limit=100)
    data = []

    for submission in top_subreddit:
        data.append(
            {
                "title": submission.title,
                "score": submission.score,
                "id": submission.id,
                "url": submission.url,
                "comments": submission.num_comments,
                "created": submission.created,
                "text": submission.selftext,
            }
        )

    return data


def transform(data):
    """Function to only keep outliers.
    Outliers are based on num of comments > 2 standard deviations from mean"""

    num_comments = [post.get("comments") for post in data]

    mean_num_comments = sum(num_comments) / len(num_comments)
    std_num_comments = (
        sum((x - mean_num_comments) ** 2 for x in num_comments)
        / len(num_comments)  # noqa
    ) ** 0.5

    return [
        post
        for post in data
        if post.get("comments") > mean_num_comments + 2 * std_num_comments
    ]


def load(data):
    """Load data into SQLite database."""

    # Create a db connection
    conn = sqlite3.connect("./data/socialetb.db")
    cur = conn.cursor()
    try:
        for post in data:
            cur.execute(
                """
                INSERT INTO social_posts (
                    id, source, social_data
                ) VALUES (:id, :source, :social_data)
""",
                {
                    "id": post.get("id"),
                    "score": post.get("score"),
                    "social_data": str(
                        {
                            "title": post.get("title"),
                            "url": post.get("url"),
                            "comments": post.get("num_comments"),
                            "created": post.get("created"),
                            "text": post.get("selftext"),
                        }
                    ),
                },
            )

    finally:
        conn.commit()
        conn.close()


def main():
    """Main."""

    # Pull data from Reddit
    data = extract()

    # Transform Reddit data
    transformed_data = transform(data)

    # Load data into database
    load(transformed_data)


if __name__ == "__main__":
    main()
