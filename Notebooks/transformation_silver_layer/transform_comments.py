import dlt
from pyspark.sql.functions import *

@dlt.view(
    name="comments_enrch_view",
    comment="cleaned and standardized comments"
)
def comments_enrch_view():
    df=spark.readStream.table("reddit_comments")
    df=df.withColumnRenamed("body","comments")
    #want to publish the data to a table in silver layer
    
    return df

dlt.create_streaming_table(
    name="silver_layer.comments_enrch"
)

dlt.create_auto_cdc_flow(
    target = "silver_layer.comments_enrch",
    source = "comments_enrch_view",
    keys = ["comment_id"],
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
