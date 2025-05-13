from utils.data_processor import fetch_non_time_series_data
from typing import Tuple, List, Dict, Any

async def fetch_targets(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
            with tvl as (SELECT 
              tvl.date,
              SUM(tvl.tvl) AS tvl_sum,
              po.owner
            FROM tvl
            JOIN project_owners po 
              ON SPLIT_PART(po.project, ' ', 1) = SPLIT_PART(tvl.project_name, ' ', 1)
            WHERE tvl.date = (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 month - 1 day')::date
            GROUP BY tvl.date, po.owner)	
	select employee, 
		users_goal, 
		mv.unique_users + mtou.unique_users as mau_actuals, 
		round((mv.unique_users + mtou.unique_users) * 100.0 / users_goal, 2) as percent_users,
		tvl_goal,
		floor(tvl.tvl_sum) as tvl_actual,
        ROUND(
            CAST(
                tvl.tvl_sum * 100.0 / NULLIF(
                    CASE
                        WHEN TRIM(COALESCE(tvl_goal, '')) = '' THEN 0
                        WHEN tvl_goal ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(tvl_goal AS numeric)
                        ELSE NULL
                    END, 0
                ) AS numeric
            ), 2) AS percent_tvl
	from employee_targets et
	left outer join mv_etherlink_owner_users mv
	on et."month" = mv."month" 
	and et.employee = mv."owner" 
	left outer join mv_tzkt_owner_users mtou 
	on et."month" = mtou."month" 
	and et.employee = mtou."user" 
	left outer join tvl on et.employee = tvl.owner
	WHERE et."month" = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month');
        """
        data = await fetch_non_time_series_data(conn, query, "report", ["employee/vertical", "mau_target", "mau_actual", "mau_percentage", "tvl_target", "tvl actual", "tvl_percentage"])
        return data, None
    except Exception as e:
        return None, e