class GlobalConfig(object):
    # Global const variables
    PRES_BASE_URL = "https://docs.google.com/presentation/d/"
    SHEET_BASE_URL = "https://docs.google.com/spreadsheets/d/"
    SLACK_ADMIN_URL = "https://hooks.slack.com/services/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"
    SLACK_REQUEST = {
        "text": ""
    }

    SLACK_HEADERS = {
        'content-type': "application/json"
    }

    HEADERS = {
        'content-type': "application/json",
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
    }

    FUNCS_BETWEEN_WEEKS = ['%dif', 'absdif']
    TIMEZONE = 'Europe/Vienna'

    # Google API
    # If modifying these scopes, delete the file token.json.
    SCOPES = ('https://www.googleapis.com/auth/presentations',
              'https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive'
              )

    BQ_PROJECT_ID = 'bigquery-208708'

    QUERIES = {
        "bq_net_rev": """
    SELECT
      activity_date,
      IF(campaign IS NULL, 'organic','attributed') AS source,
      LOWER(store) as platform,
      SUM(iap_net_revenue) AS net_iap,
      SUM(ad_net_revenue) AS net_ad,
      SUM(iap_net_revenue) + SUM(ad_net_revenue) AS net_income
    FROM
      `bigquery-208708.(****).daily_user_state`
    WHERE
      activity_date <= CURRENT_DATE()
    GROUP BY
      1,
      2,
      3
    ORDER BY
      1 DESC,
      2,
      3
    """,
        "bq_net_ext": """
    SELECT
      activity_date,
      IF(campaign IS NULL, 'organic','attributed') AS source,
      LOWER(store) as platform,
      SUM(iap_net_revenue) AS net_iap,
      SUM(ad_net_revenue) AS net_ad,
      SUM(iap_net_revenue) + SUM(ad_net_revenue) AS net_income,
      SAFE_DIVIDE(SUM(iap_net_revenue), SUM(iap_net_revenue) + SUM(ad_net_revenue)) * 100 AS perc_iap,
      SAFE_DIVIDE(SUM(ad_net_revenue), SUM(iap_net_revenue) + SUM(ad_net_revenue)) * 100 AS perc_ads
    FROM
      `bigquery-208708.(****).daily_user_state`
    WHERE
      activity_date < CURRENT_DATE()
    GROUP BY
      1,
      2,
      3
    ORDER BY
      1 desc,
      2,
      3
    """,
        "bq_net_sum": """
    SELECT
      activity_date,
      IF(campaign IS NULL,'organic','attributed') AS source,
      SUM(iap_net_revenue) AS net_iap,
      SUM(ad_net_revenue) AS net_ad,
      SUM(iap_net_revenue) + SUM(ad_net_revenue) AS net_income
    FROM
      `bigquery-208708.(****).daily_user_state`
    WHERE
      activity_date < CURRENT_DATE()
    GROUP BY
      1,
      2
    ORDER BY
      1 DESC,
      2
      """,


        "bq_net_sum_ext": """
    SELECT
      activity_date,
      IF(campaign IS NULL, 'organic','attributed') AS source,
      lower(store) as platform,
      SUM(iap_net_revenue) AS net_iap,
      SUM(ad_net_revenue) AS net_ad,
      SUM(iap_net_revenue) + SUM(ad_net_revenue) AS net_income,
      SAFE_DIVIDE(SUM(iap_net_revenue), SUM(iap_net_revenue) + SUM(ad_net_revenue)) * 100 AS perc_iap,
      SAFE_DIVIDE(SUM(ad_net_revenue), SUM(iap_net_revenue) + SUM(ad_net_revenue)) * 100 AS perc_ads
    FROM
      `bigquery-208708.(****).daily_user_state`
    WHERE
      activity_date < CURRENT_DATE()
    GROUP BY
      1,
      2,
      3
    ORDER BY
      1 DESC,
      2,
      3
      """,
        "bq_net_uid": """
    SELECT
      activity_date,
      IF(LOWER(campaign) <> 'none', 'attributed','organic') AS source,
      LOWER(store) as platform,
      SUM(iap_net_revenue) AS net_iap,
      SUM(ad_net_revenue) AS net_ad,
      SUM(iap_net_revenue) + SUM(ad_net_revenue) AS net_income,
      COUNT(distinct uid) as retained_players
    FROM
      `bigquery-208708.(****).daily_user_state`
    WHERE
      activity_date < CURRENT_DATE()
    GROUP BY
      1,
      2,
      3
    ORDER BY
      1 DESC,
      2,
      3
    """,
        "bq_net_uid_v2": """
    SELECT
      activity_date,
      IF(LOWER(campaign) <> 'none', 'attributed', 'organic') AS source,
      LOWER(store) as platform,
      SUM(iap_net_revenue) AS net_iap,
      SUM(ad_net_revenue) AS net_ad,
      SUM(iap_net_revenue) + SUM(ad_net_revenue) AS net_income,
      COUNT(distinct uid) as retained_players,
      COUNT(DISTINCT IF(creation_date = activity_date, uid, NULL)) as new_players
    FROM
      `bigquery-208708.(****).daily_user_state`
    WHERE
      activity_date < CURRENT_DATE()
    GROUP BY
      1,
      2,
      3
    ORDER BY
      1 DESC,
      2,
      3
    """,
        "bq_net_uid_v2_wo_samsung": """
    SELECT
      activity_date,
      IF(LOWER(campaign) <> 'none', 'attributed', 'organic') AS source,
      LOWER(store) as platform,
      SUM(iap_net_revenue) AS net_iap,
      SUM(ad_net_revenue) AS net_ad,
      SUM(iap_net_revenue) + SUM(ad_net_revenue) AS net_income,
      COUNT(distinct uid) as retained_players,
      COUNT(DISTINCT IF(creation_date = activity_date, uid, NULL)) as new_players
    FROM
      `bigquery-208708.(****).daily_user_state`
    WHERE
      activity_date < CURRENT_DATE() and store <> 'samsung' 
    GROUP BY
      1,
      2,
      3
    ORDER BY
      1 DESC,
      2,
      3
    """
    }
