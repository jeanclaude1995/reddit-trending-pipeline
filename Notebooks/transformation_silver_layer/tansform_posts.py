import dlt
from pyspark.sql.functions import *

@dlt.view(
    name="posts_enrch_view",
    comment="cleaned and standardized posts"
)
def posts_enrch_view():
    df=spark.readStream.table("reddit_posts")
    df=df.withColumn("author",lower(col("author")))
    #want to publish the data to a table in silver layer
    
    return df

dlt.create_streaming_table(
    name="silver_layer.posts_enrch"
)

dlt.create_auto_cdc_flow(
    target = "silver_layer.posts_enrch",
    source = "posts_enrch_view",
    keys = ["post_id"],
    sequence_by = "extraction_timestamp",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 2,
    track_history_column_list = None,
    track_history_except_column_list = None,
    name = None,
    once = False
)
