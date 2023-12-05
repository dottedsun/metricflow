-- Compute Metrics via Expressions
SELECT
  instant_lux_booking_value AS instant_lux_booking_value
FROM (
  -- Compute Metrics via Expressions
  SELECT
    average_booking_value * bookings / NULLIF(booking_value, 0) AS instant_lux_booking_value
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      MAX(subq_42.average_booking_value) AS average_booking_value
      , MAX(subq_54.bookings) AS bookings
      , MAX(subq_59.booking_value) AS booking_value
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements:
      --   ['average_booking_value']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        AVG(average_booking_value) AS average_booking_value
      FROM (
        -- Join Standard Outputs
        -- Pass Only Elements:
        --   ['average_booking_value', 'listing__is_lux_latest']
        SELECT
          listings_latest_src_10004.is_lux AS listing__is_lux_latest
          , bookings_source_src_10001.booking_value AS average_booking_value
        FROM ***************************.fct_bookings bookings_source_src_10001
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_10004
        ON
          bookings_source_src_10001.listing_id = listings_latest_src_10004.listing_id
      ) subq_38
      WHERE listing__is_lux_latest
    ) subq_42
    CROSS JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements:
      --   ['bookings']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        SUM(bookings) AS bookings
      FROM (
        -- Join Standard Outputs
        -- Pass Only Elements:
        --   ['bookings', 'listing__is_lux_latest']
        SELECT
          listings_latest_src_10004.is_lux AS listing__is_lux_latest
          , subq_45.bookings AS bookings
        FROM (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements:
          --   ['bookings', 'listing']
          SELECT
            listing_id AS listing
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_10001
        ) subq_45
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_10004
        ON
          subq_45.listing = listings_latest_src_10004.listing_id
      ) subq_50
      WHERE listing__is_lux_latest
    ) subq_54
    CROSS JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['booking_value']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        SUM(booking_value) AS booking_value
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_59
  ) subq_60
) subq_61
