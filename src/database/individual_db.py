from utils.data_processor import fetch_non_time_series_data
from typing import Tuple, List, Dict, Any

async def fetch_owner_actual_users(owner: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
        SELECT 
            month,
            SUM(unique_users) AS total_users
        FROM (
            SELECT 
                month,
                unique_users
            FROM 
                mv_etherlink_owner_users
            WHERE 
                UPPER(owner) = UPPER($1) 
                AND month > current_date - INTERVAL '8 months'
            UNION ALL
            SELECT 
                month,
                unique_users
            FROM 
                mv_tzkt_owner_users m
            WHERE 
                UPPER(m.user) = UPPER($1) 
                AND month > current_date - INTERVAL '8 months'
        ) combined_data
        GROUP BY 
            month
        ORDER BY 
            month
        """
        data = await fetch_non_time_series_data(conn, query, "owner_users", ["time", "total_users"], owner)
        return data, None
    except Exception as e:
        return None, e
        
async def fetch_owner_predicted_users(owner: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT month, CAST(users_forecast AS TEXT) as users_goal
          FROM employee_targets et 
          WHERE UPPER(employee) = UPPER($1) 
          and month > current_date - INTERVAL '8 months'
          and month < current_date + interval '4 months'
          ORDER BY month ASC
        """
        data = await fetch_non_time_series_data(conn, query, "predicted_users", ["time", "forecast"], owner)
        return data, None
    except Exception as e:
        return None, e

async def fetch_owner_goals_users(owner: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT month, CAST(users_goal AS TEXT) as users_goal
          FROM employee_targets et 
          WHERE UPPER(employee) = UPPER($1) 
          and month > current_date - INTERVAL '8 months'
          and month < current_date + interval '4 months'
          ORDER BY month ASC
        """
        data = await fetch_non_time_series_data(conn, query, "target_users", ["time", "target"], owner)
        return data, None
    except Exception as e:
        return None, e
        
async def fetch_owner_top_projects_users(owner: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT m.project, user_count, layer FROM mv_etherlink_projects_users m
          join project_owners po 
          on m.project = po.project 
          WHERE UPPER(po."owner") =  UPPER($1)
          and month = date_trunc('month', current_date - interval '1 month')
          UNION ALL 
          SELECT m1.project, user_count, layer FROM mv_tzkt_projects_users m1
          join project_owners po 
          on m1.project = po.project 
          WHERE UPPER(po."owner") =  UPPER($1)
          and month = date_trunc('month', current_date - interval '1 month')
          ORDER BY user_count DESC;
        """
        data = await fetch_non_time_series_data(conn, query, "owner_users_projects", ["Project", "Active Wallets", "Layer"], owner)
        return data, None
    except Exception as e:
        return None, e

async def fetch_owner_actual_tvl(owner: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          WITH last_day_tvl AS (
          SELECT 
	        (DATE_TRUNC('month', current_date) - INTERVAL '1 day') - INTERVAL '1 month' * (n - 1) AS date
          FROM 
	        GENERATE_SERIES(1, 7) n
          ORDER BY date
          )
          SELECT 
          date_trunc('month', tvl.date) as month,
          sum(tvl) as tvl_res
          FROM last_day_tvl t
          join tvl 
          on tvl."date" = t.date
          JOIN project_owners po
          ON SPLIT_PART(po.project, ' ', 1) = SPLIT_PART(tvl.project_name, ' ', 1)
          WHERE  upper(po."owner") = upper($1)
          group by month
          order by month asc
        """
        data = await fetch_non_time_series_data(conn, query, "owner_tvl", ["time", "actual"], owner)
        return data, None
    except Exception as e:
        return None, e   

async def fetch_owner_predicted_tvl(owner: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT month, CAST(tvl_forecast AS TEXT) as tvl_goal
          FROM employee_targets et 
          WHERE UPPER(employee) = UPPER($1) 
          and month > current_date - INTERVAL '8 months'
          and month < current_date + interval '4 months'
          ORDER BY month ASC
        """
        data = await fetch_non_time_series_data(conn, query, "owner_predicted_tvl", ["time", "forecast"], owner)
        return data, None
    except Exception as e:
        return None, e 

async def fetch_owner_goals_tvl(owner: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT month, CAST(tvl_goal AS TEXT) as tvl_goal
          FROM employee_targets et 
          WHERE UPPER(employee) = UPPER($1) 
          and month > current_date - INTERVAL '8 months'
          and month < current_date + interval '4 months'
          ORDER BY month ASC
        """
        data = await fetch_non_time_series_data(conn, query, "owner_goal_tvl", ["time", "target"], owner)
        return data, None
    except Exception as e:
        return None, e 

async def fetch_owner_top_projects_tvl(owner: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          WITH last_day_tvl AS (
          SELECT 
	        (DATE_TRUNC('month', current_date) - INTERVAL '1 day') - INTERVAL '1 month' * (n - 1) AS date
          FROM 
	        GENERATE_SERIES(1, 7) n
          ORDER BY date
          )
          SELECT 
            date_trunc('month', tvl.date) as month,
            sum(tvl) as tvl_res,
            project
          FROM last_day_tvl t
          join tvl 
          on tvl."date" = t.date
          JOIN project_owners po
          ON SPLIT_PART(po.project, ' ', 1) = SPLIT_PART(tvl.project_name, ' ', 1)
          WHERE  upper(po."owner") = upper($1)
          group by month, project
          order by month asc, tvl_res desc;
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["time", "tvl_res", "project"], owner)
        return data, None
    except Exception as e:
        return None, e
        