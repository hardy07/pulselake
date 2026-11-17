-- Dataset: top_songs. Horizontal bar: song_title vs listens; artist in tooltip.
SELECT song_id, song_title, artist, count(*) AS listens,
       round(sum(duration_seconds) / 3600.0, 2) AS listening_hours
FROM workspace.pulselake_gold.fact_listens
GROUP BY song_id, song_title, artist
ORDER BY listens DESC, song_id
LIMIT 10;
