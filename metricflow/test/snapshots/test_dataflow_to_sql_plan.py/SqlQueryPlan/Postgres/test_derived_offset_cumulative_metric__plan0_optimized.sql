-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements:
  --   ['bookers', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_16.metric_time__day AS metric_time__day
    , COUNT(DISTINCT subq_15.bookers) AS every_2_days_bookers_2_days_ago
  FROM (
    -- Date Spine
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine subq_17
    GROUP BY
      ds
  ) subq_16
  INNER JOIN (
    -- Join Self Over Time Range
    SELECT
      subq_13.metric_time__day AS metric_time__day
      , bookings_source_src_10001.guest_id AS bookers
    FROM (
      -- Date Spine
      SELECT
        ds AS metric_time__day
      FROM ***************************.mf_time_spine subq_14
      GROUP BY
        ds
    ) subq_13
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_10001
    ON
      (
        bookings_source_src_10001.ds <= subq_13.metric_time__day
      ) AND (
        bookings_source_src_10001.ds > subq_13.metric_time__day - MAKE_INTERVAL(days => 2)
      )
  ) subq_15
  ON
    subq_16.metric_time__day - MAKE_INTERVAL(days => 2) = subq_15.metric_time__day
  GROUP BY
    subq_16.metric_time__day
) subq_21
