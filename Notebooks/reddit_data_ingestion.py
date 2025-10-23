import asyncpraw
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
import logging


class AsyncRedditDataExtractor:
    def __init__(self, client_id: str, client_secret: str, user_agent: str,
            spark_session: SparkSession = None, catalog_name: str = "main",
            schema_name: str = "reddit_bronze", table_prefix: str = "reddit"):
        """
        Initialize async Reddit API client and Databricks integration
        """
        self.reddit = asyncpraw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )

        # Spark / Databricks config
        self.spark = spark_session or SparkSession.builder.appName("AsyncRedditDataExtractor").getOrCreate()
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_prefix = table_prefix

        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Create schema if missing
        self._create_schema()

        # Define schemas
        self._define_schemas()

    def _create_schema(self):
        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{self.schema_name}")
        except Exception as e:
            self.logger.error(f"Error creating schema: {str(e)}")

    def _define_schemas(self):
        self.posts_schema = StructType([
            StructField("post_id", StringType(), False),
            StructField("title", StringType(), True),
            StructField("author", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("upvote_ratio", DoubleType(), True),
            StructField("num_comments", IntegerType(), True),
            StructField("created_utc", TimestampType(), True),
            StructField("url", StringType(), True),
            StructField("permalink", StringType(), True),
            StructField("subreddit", StringType(), True),
            StructField("is_self", BooleanType(), True),
            StructField("selftext", StringType(), True),
            StructField("link_flair_text", StringType(), True),
            StructField("over_18", BooleanType(), True),
            StructField("spoiler", BooleanType(), True),
            StructField("locked", BooleanType(), True),
            StructField("stickied", BooleanType(), True),
            StructField("extraction_timestamp", TimestampType(), True),
            StructField("extraction_date", StringType(), True)
        ])

        self.comments_schema = StructType([
            StructField("comment_id", StringType(), False),
            StructField("post_id", StringType(), True),
            StructField("author", StringType(), True),
            StructField("body", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("created_utc", TimestampType(), True),
            StructField("permalink", StringType(), True),
            StructField("parent_id", StringType(), True),
            StructField("is_submitter", BooleanType(), True),
            StructField("stickied", BooleanType(), True),
            StructField("controversiality", IntegerType(), True),
            StructField("depth", IntegerType(), True),
            StructField("subreddit", StringType(), True),
            StructField("extraction_timestamp", TimestampType(), True),
            StructField("extraction_date", StringType(), True)
        ])

    async def extract_recent_posts(self, subreddit_names: List[str], hours_back: int = 24,
                            limit_per_subreddit: int = 100) -> List[Dict[str, Any]]:
        """Extract posts within the last N hours from multiple subreddits"""
        all_posts = []
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        for subreddit_name in subreddit_names:
            try:
                self.logger.info(f"Extracting recent posts from r/{subreddit_name}")
                subreddit = await self.reddit.subreddit(subreddit_name)

                async for submission in subreddit.new(limit=limit_per_subreddit):
                    post_time = datetime.fromtimestamp(submission.created_utc)
                    if post_time < cutoff_time:
                        break

                    extraction_time = datetime.now()
                    post_data = {
                        "post_id": submission.id,
                        "title": submission.title,
                        "author": str(submission.author) if submission.author else "[deleted]",
                        "score": submission.score,
                        "upvote_ratio": submission.upvote_ratio,
                        "num_comments": submission.num_comments,
                        "created_utc": datetime.fromtimestamp(submission.created_utc),
                        "url": submission.url,
                        "permalink": f"https://reddit.com{submission.permalink}",
                        "subreddit": str(submission.subreddit),
                        "is_self": submission.is_self,
                        "selftext": submission.selftext if submission.is_self else None,
                        "link_flair_text": submission.link_flair_text,
                        "over_18": submission.over_18,
                        "spoiler": submission.spoiler,
                        "locked": submission.locked,
                        "stickied": submission.stickied,
                        "extraction_timestamp": extraction_time,
                        "extraction_date": extraction_time.strftime("%Y-%m-%d")
                    }
                    all_posts.append(post_data)

            except Exception as e:
                self.logger.error(f"Error extracting posts from r/{subreddit_name}: {e}")
                continue

        self.logger.info(f"Total posts extracted: {len(all_posts)}")
        return all_posts

    async def extract_comments_for_posts(self, post_ids: List[str], limit_per_post: int = 50) -> List[Dict[str, Any]]:
        """Extract comments for a list of posts"""
        all_comments = []

        for post_id in post_ids:
            try:
                submission = await self.reddit.submission(id=post_id)
                await submission.load()
                await submission.comments.replace_more(limit=0)  # flatten "MoreComments"

                extraction_time = datetime.now()

                for comment in submission.comments.list()[:limit_per_post]:
                    comment_data = {
                        "comment_id": comment.id,
                        "post_id": submission.id,
                        "author": str(comment.author) if comment.author else "[deleted]",
                        "body": comment.body,
                        "score": comment.score,
                        "created_utc": datetime.fromtimestamp(comment.created_utc),
                        "permalink": f"https://reddit.com{comment.permalink}",
                        "parent_id": comment.parent_id,
                        "is_submitter": getattr(comment, "is_submitter", False),
                        "stickied": getattr(comment, "stickied", False),
                        "controversiality": getattr(comment, "controversiality", 0),
                        "depth": getattr(comment, "depth", 0),
                        "subreddit": str(comment.subreddit),
                        "extraction_timestamp": extraction_time,
                        "extraction_date": extraction_time.strftime("%Y-%m-%d")
                    }
                    all_comments.append(comment_data)

            except Exception as e:
                self.logger.error(f"Error extracting comments for post {post_id}: {e}")
                continue

        self.logger.info(f"Total comments extracted: {len(all_comments)}")
        return all_comments

    def save_posts_to_databricks(self, posts_data: List[Dict[str, Any]], table_name: str = "posts"):
        """Save posts data to Databricks"""
        if not posts_data:
            self.logger.warning("No posts data to save")
            return

        df = self.spark.createDataFrame(posts_data, schema=self.posts_schema)
        full_table = f"{self.catalog_name}.{self.schema_name}.{self.table_prefix}_{table_name}"
        df.write.mode("append").option("mergeSchema", "true").partitionBy("extraction_date").saveAsTable(full_table)
        self.logger.info(f"Saved {len(posts_data)} posts to {full_table}")

    def save_comments_to_databricks(self, comments_data: List[Dict[str, Any]], table_name: str = "comments"):
        """Save comments data to Databricks"""
        if not comments_data:
            self.logger.warning("No comments data to save")
            return

        df = self.spark.createDataFrame(comments_data, schema=self.comments_schema)
        full_table = f"{self.catalog_name}.{self.schema_name}.{self.table_prefix}_{table_name}"
        df.write.mode("append").option("mergeSchema", "true").partitionBy("extraction_date").saveAsTable(full_table)
        self.logger.info(f"Saved {len(comments_data)} comments to {full_table}")

    async def close(self):
        """Close async Reddit client"""
        await self.reddit.close()


# -------------------------
# Main for Databricks
# -------------------------
async def main():
    extractor = AsyncRedditDataExtractor(
        client_id="eegt6mCVIVgEwUwwrPf8vw",
        client_secret="95xML3ZZRgRmPVS3Bdag-_pNRY-rlw",
        user_agent="databricks:reddit_data_pipeline:1.0 by u/Subject-Elk5130",
        catalog_name="workspace",
        schema_name="reddit_bronze"
    )

    target_subreddits = ["technology", "dataengineering", "MachineLearning"]

    # Extract posts
    posts = await extractor.extract_recent_posts(target_subreddits, hours_back=24, limit_per_subreddit=50)
    extractor.save_posts_to_databricks(posts)

    # Extract comments for those posts
    post_ids = [p["post_id"] for p in posts]
    comments = await extractor.extract_comments_for_posts(post_ids, limit_per_post=100)
    extractor.save_comments_to_databricks(comments)

    await extractor.close()


if __name__ == "__main__":
    await main()
