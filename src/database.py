import os
import asyncpg
from typing import Tuple, List, Dict, Any

db_pool = None

async def init_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        min_size=1,
        max_size=10
    )

async def fetch_team_goals_users(department: str) -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
        try:
            query = """
            SELECT month, CAST(users_goal AS TEXT) AS users_goal
            FROM team_targets 
            WHERE UPPER(department) = UPPER($1)
            AND month > current_date - INTERVAL '8 months'
            AND month < current_date + INTERVAL '5 months'
            ORDER BY month ASC
            """
            rows = await conn.fetch(query, department)
            data = [{"time": row["month"], "target": row["users_goal"]} for row in rows]
            return data, None
        except Exception as e:
            return None, e

async def fetch_team_actual_users(department: str) -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
        try:
            query = """
              SELECT month, CAST(actual_users AS TEXT) as actual_users
              FROM team_targets 
              WHERE UPPER(department) = UPPER($1) 
              and month <= DATE_TRUNC('month', CURRENT_DATE)
			  and month > current_date - INTERVAL '8 months'
              ORDER BY month ASC
            """
            rows = await conn.fetch(query, department)
            data = [{"time": row["month"], "actual": row["actual_users"]} for row in rows]
            return data, None
        except Exception as e:
            return None, e

async def fetch_team_predicted_users(department: str) -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
        try:
            query = """
              SELECT month, CAST(predicted_users AS TEXT) as predicted_users
              FROM team_targets 
              WHERE UPPER(department) = UPPER($1) 
              and month >= DATE_TRUNC('month', CURRENT_DATE)
              and month < current_date + interval '5 months'
              ORDER BY month ASC
            """
            rows = await conn.fetch(query, department)
            data = [{"time": row["month"], "predicted": row["predicted_users"]} for row in rows]
            return data, None
        except Exception as e:
            return None, e

async def fetch_owner_actual_users(owner: str) -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
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
            rows = await conn.fetch(query, owner)
            data = [{"time": row["month"], "total_users": row["total_users"]} for row in rows]
            return data, None
        except Exception as e:
            return None, e
        
async def fetch_owner_predicted_users(owner: str) -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
        try:
            query = """
              SELECT month, CAST(users_forecast AS TEXT) as users_goal
              FROM employee_targets et 
              WHERE UPPER(employee) = UPPER($1) 
              and month > current_date - INTERVAL '8 months'
              and month < current_date + interval '4 months'
              ORDER BY month ASC
            """
            rows = await conn.fetch(query, owner)
            data = [{"time": row["month"], "forecast": row["users_goal"]} for row in rows]
            return data, None
        except Exception as e:
            return None, e

async def fetch_owner_goals_users(owner: str) -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
        try:
            query = """
              SELECT month, CAST(users_forecast AS TEXT) as users_goal
              FROM employee_targets et 
              WHERE UPPER(employee) = UPPER($1) 
              and month > current_date - INTERVAL '8 months'
              and month < current_date + interval '4 months'
              ORDER BY month ASC
            """
            rows = await conn.fetch(query, owner)
            data = [{"time": row["month"], "target": row["users_goal"]} for row in rows]
            return data, None
        except Exception as e:
            return None, e
        
async def fetch_owner_top_projects_users(owner: str) -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
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
            rows = await conn.fetch(query, owner)
            data = [
                {"Project": row["project"], "Active Wallets": row["user_count"], "Layer": row["layer"]}
                for row in rows
            ]
            return data, None
        except Exception as e:
            return None, e

async def fetch_owner_actual_tvl(owner: str) -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
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
            rows = await conn.fetch(query, owner)
            data = [{"time": row["month"], "actual": row["tvl_res"]} for row in rows]
            return data, None
        except Exception as e:
            return None, e   

async def fetch_owner_predicted_tvl(owner: str) -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
        try:
            query = """
              SELECT month, CAST(tvl_forecast AS TEXT) as tvl_goal
              FROM employee_targets et 
              WHERE UPPER(employee) = UPPER($1) 
              and month > current_date - INTERVAL '8 months'
              and month < current_date + interval '4 months'
              ORDER BY month ASC
            """
            rows = await conn.fetch(query, owner)
            data = [{"time": row["month"], "forecast": row["tvl_goal"]} for row in rows]
            return data, None
        except Exception as e:
            return None, e 

async def fetch_owner_goals_tvl(owner: str) -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
        try:
            query = """
              SELECT month, CAST(tvl_goal AS TEXT) as tvl_goal
              FROM employee_targets et 
              WHERE UPPER(employee) = UPPER($1) 
              and month > current_date - INTERVAL '8 months'
              and month < current_date + interval '4 months'
              ORDER BY month ASC
            """
            rows = await conn.fetch(query, owner)
            data = [{"time": row["month"], "target": row["tvl_goal"]} for row in rows]
            return data, None
        except Exception as e:
            return None, e 

async def fetch_owner_top_projects_tvl(owner: str) -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
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
            rows = await conn.fetch(query, owner)
            data = [
                {"time": row["month"], "TVL": row["tvl_res"], "project": row["project"]}
                for row in rows
            ]
            return data, None
        except Exception as e:
            return None, e
        
async def fetch_top_projects_transactions() -> Tuple[List[Dict[str, Any]], Exception]:
    async with db_pool.acquire() as conn:
        try:
            query = """
            SELECT project, tx_count, layer 
            FROM mv_etherlink_projects_transactions 
            WHERE month = (SELECT MAX(month) FROM mv_etherlink_projects_transactions)
            AND tx_count > 0
            UNION ALL
            SELECT project, tx_count, layer 
            FROM mv_tzkt_projects_transactions
            WHERE month = (SELECT MAX(month) FROM mv_tzkt_projects_transactions)
            AND tx_count > 0
            ORDER BY tx_count DESC
            """
            rows = await conn.fetch(query)
            data = [
                {"Project": row["project"], "Total Transactions": row["tx_count"], "Layer": row["layer"]}
                for row in rows
            ]
            return data, None
        except Exception as e:
            return None, e
