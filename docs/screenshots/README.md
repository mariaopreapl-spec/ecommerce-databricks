# Screenshots to add

Save each image here under the file name below. Then remove the comment markers around the
**Screenshots** section in the main README, and the images will appear.

| File name | Where in Databricks | What it should show |
|---|---|---|
| `silver_pipeline_graph.png` | Jobs & Pipelines → DLT_Silver_Layer → last successful run | The full graph: stage → trans → silver / quarantine for all datasets |
| `silver_expectations.png` | Same run → click `silver_orders` → **Data quality** tab | Expectations with passed/failed record counts |
| `bronze_job_for_each.png` | Jobs & Pipelines → bronze job → last run | The For each task with its 9 iterations, plus the stream task |
| `gold_dashboard.png` | A Databricks AI/BI dashboard, Power BI or Tableau | 2–3 charts on the gold marts, e.g. monthly revenue (Black Friday peak), margin by category, web funnel |

Tips:
- Take the screenshots in the light theme, at about 1400 px wide, so they stay readable on GitHub.
- Crop out the browser bar and your e-mail address.
