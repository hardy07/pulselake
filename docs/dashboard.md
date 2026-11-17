# PulseLake dashboard

Build Gold first, then create an AI/BI dashboard in your workspace. Select your
SQL warehouse and add one dataset for each SQL file in `databricks/dashboard/`.
The queries use catalog `workspace`; change this if you configured another catalog.

| Dataset | File | Visualization |
|---|---|---|
| Summary | `01_summary.sql` | Four counters: total_listens, active_listeners, songs_played, listening_hours. |
| Top songs | `02_top_songs.sql` | Horizontal bar: song_title and listens. |
| Daily activity | `03_daily_activity.sql` | Bar: event_date and listens. |
| Geography | `04_country_activity.sql` | Country and listens. |
| Subscription | `05_subscription_activity.sql` | Subscription and listens. |
| Data quality | `06_data_quality.sql` | Rejection reasons and counts. |

Run each dataset before wiring widgets. The summary returns one aggregate row;
select the actual metric as the value, using SUM or MAX rather than counting rows.
Active listeners are users with listens, not the full user dimension.

After a successful pipeline run, refresh the dashboard using its refresh arrow.
Browser reload can retain cached results. If Gold changes but the dashboard does
not, run the summary dataset and check widget source fields and date filters.

For automatic refresh, publish the dashboard and add the dependent Dashboard task
explained in [the pipeline guide](pipeline.md). The default bundle has four tasks;
automatic refresh has not been verified for this renamed deployment.

Historical demo metrics are documented in the README. A new deployment begins
with fresh tables; metrics vary with the batches generated or replayed.
