# reddit-trending-pipeline
Reddit trending insights pipeline using Databricks DLT &amp; Python async

🚀 Reddit Trending Insights Data Pipeline

This project automates the process of fetching and transforming Reddit data into actionable insights — identifying Top 10 Trending Posts of the Week using Databricks, Delta Live Tables (DLT), and Python (async).

🧠 Overview

Reddit is one of the most active social platforms with dynamic user engagement.
This project builds a modern data pipeline that continuously fetches Reddit data, cleans and transforms it, and surfaces trending insights for further analysis or dashboarding.

🏗️ Architecture
Layer	Description
Bronze	Raw Reddit data ingestion using the Reddit API and Python’s async capabilities.
Silver	Cleaned and standardized data via Delta Live Tables (DLT) transformations.
Gold	Aggregated, business-ready insights for the top 10 trending Reddit posts.

Workflow:

Reddit API → Bronze (Raw) → Silver (Cleaned) → Gold (Trending Insights)

⚙️ Technologies Used

🧩 Databricks Workflows & Delta Live Tables (DLT)

🐍 Python (AsyncIO, Requests)

🔥 PySpark / SQL

💾 Delta Lake (Bronze–Silver–Gold)

📊 Databricks Lakehouse Platform
