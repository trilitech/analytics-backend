from utils.data_processor import fetch_non_time_series_data
from typing import Tuple, List, Dict, Any
from database.db_utils import db_pool

async def fetch_team_goals_users(department: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
        SELECT month, CAST(users_goal AS TEXT) AS users_goal
        FROM team_targets 
        WHERE UPPER(department) = UPPER($1)
        AND month > current_date - INTERVAL '8 months'
        AND month < current_date + INTERVAL '5 months'
        ORDER BY month ASC
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["time", "target"], department)
        return data, None
    except Exception as e:
        return None, e

async def fetch_team_actual_users(department: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT month, CAST(actual_users AS TEXT) as actual_users
          FROM team_targets 
          WHERE UPPER(department) = UPPER($1) 
          and month <= DATE_TRUNC('month', CURRENT_DATE)
		  and month > current_date - INTERVAL '8 months'
          ORDER BY month ASC
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["time", "actual"], department)
        return data, None
    except Exception as e:
        return None, e

async def fetch_team_predicted_users(department: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT month, CAST(predicted_users AS TEXT) as predicted_users
          FROM team_targets 
          WHERE UPPER(department) = UPPER($1) 
          and month >= DATE_TRUNC('month', CURRENT_DATE)
          and month < current_date + interval '5 months'
          ORDER BY month ASC
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["time", "predicted"], department)
        return data, None
    except Exception as e:
        return None, e

async def fetch_team_tzkt_users(department: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT 
            date(month) as month,
            unique_users
          FROM 
            mv_tzkt_team_users
          WHERE 
            month > (CURRENT_DATE - INTERVAL '8 months')
            AND UPPER(department) = UPPER($1)
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["time", "total_users"], department)
        return data, None
    except Exception as e:
        return None, e
    
async def fetch_team_etherlink_users(department: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT 
            date(month) as month,
            unique_users
          FROM 
            mv_etherlink_team_users
          WHERE 
            month > (CURRENT_DATE - INTERVAL '8 months')
            AND UPPER(department) = UPPER($1)
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["time", "total_etherlink_users"], department)
        return data, None
    except Exception as e:
        return None, e

async def fetch_team_tzkt_transactions(department: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT 
            date(month) as month, 
            sum(tx_count) as total_tkzt_tx_count
          FROM 
            mv_tzkt_projects_transactions 
          WHERE 
            UPPER(department) = UPPER($1) 
            AND month != (SELECT MAX(month) FROM mv_tzkt_projects_transactions)
            and month > current_date - INTERVAL '8 months'
          group by month
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["time", "team_transactions"], department)
        return data, None
    except Exception as e:
        return None, e
    
async def fetch_team_etherlink_transactions(department: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT 
            date(month) as month, 
            sum(tx_count) as total_etherlink_tx_count
          FROM 
            mv_etherlink_projects_transactions 
          WHERE 
            UPPER(department) = UPPER($1) 
            AND month != (SELECT MAX(month) FROM mv_etherlink_projects_transactions)
            and month > current_date - INTERVAL '8 months'
          group by month
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["time", "team_transactions_etherlink"], department)
        return data, None
    except Exception as e:
        return None, e

async def fetch_team_projects_transactions(department: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
              SELECT project, tx_count, layer FROM mv_etherlink_projects_transactions 
              WHERE UPPER(department) = UPPER($1) 
              AND month = (SELECT MAX(month) FROM mv_etherlink_projects_transactions)
              UNION ALL 
              SELECT project, tx_count, layer FROM mv_tzkt_projects_transactions 
              WHERE UPPER(department) = UPPER($1) 
              and month = (SELECT MAX(month) FROM mv_tzkt_projects_transactions)
              ORDER BY tx_count DESC;
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["Project", "Total Transactions", "Layer"], department)
        return data, None
    except Exception as e:
        return None, e
    
async def fetch_team_projects_users(department: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
              SELECT project, user_count, layer FROM mv_etherlink_projects_users
              WHERE UPPER(department) = UPPER($1) 
              AND month = (SELECT MAX(month) FROM mv_etherlink_projects_transactions) 
              UNION ALL 
              SELECT project, user_count, layer FROM mv_tzkt_projects_users
              WHERE UPPER(department) = UPPER($1) 
              and month = (SELECT MAX(month) FROM mv_tzkt_projects_transactions)
              ORDER BY user_count DESC;
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["Project", "Active Wallets", "Layer"], department)
        return data, None
    except Exception as e:
        return None, e
    
async def fetch_team_projects_transactions_split(department: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
              select date(month) as month, tx_count, project 
              from mv_etherlink_projects_transactions
              where UPPER(department) = UPPER($1) 
              AND month != (SELECT MAX(month) FROM mv_etherlink_projects_transactions)
              and month > current_date - INTERVAL '8 months'
			  order by month
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["time", "transactions_etherlink", "project"], department)
        return data, None
    except Exception as e:
        return None, e
    
async def fetch_team_projects_users_split(department: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
              select date(month) as month, user_count, project
       		  from mv_etherlink_projects_users mepu
              where 
        	  UPPER(department) = UPPER($1) 
      		  AND month != (SELECT MAX(month) FROM mv_etherlink_projects_transactions)
              and month > current_date - INTERVAL '8 months'   
  			  order by month
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["time", "user_count", "project"], department)
        return data, None
    except Exception as e:
        return None, e
