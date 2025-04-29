from utils.data_processor import fetch_non_time_series_data
from typing import Tuple, List, Dict, Any
from database.db_utils import db_pool

async def fetch_etherlink_transactions(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select date(month), transaction_count from mv_etherlink_monthly_transactions
        """
        data = await fetch_non_time_series_data(conn, query, "etherlink_transactions", ["time", "etherlink_transactions"])
        return data, None
    except Exception as e:
        return None, e

async def fetch_tzkt_transactions(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select date(month), transaction_count from mv_tzkt_monthly_transactions
        """
        data = await fetch_non_time_series_data(conn, query, "tzkt_transactions", ["time", "tzkt_transactions"])
        return data, None
    except Exception as e:
        return None, e

async def fetch_etherlink_users(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select * from mv_etherlink_monthly_users
        """
        data = await fetch_non_time_series_data(conn, query, "etherlink_users", ["time", "etherlink_unique_users"])
        return data, None
    except Exception as e:
        return None, e
    
async def fetch_tzkt_users(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select * from mv_tzkt_monthly_users
        """
        data = await fetch_non_time_series_data(conn, query, "active_wallets", ["time", "tzkt_unique_users"])
        return data, None
    except Exception as e:
        return None, e

async def fetch_bot_users(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select * from mv_bot_wallets
        """
        data = await fetch_non_time_series_data(conn, query, "bot_users", ["time", "bot_users"])
        return data, None
    except Exception as e:
        return None, e   

async def fetch_bot_transactions(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select date(month), transaction_count from mv_bot_transactions
        """
        data = await fetch_non_time_series_data(conn, query, "bot_transactions", ["time", "bot_transactions"])
        return data, None
    except Exception as e:
        return None, e
    
async def fetch_etherlink_recurring_users(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select * from mv_etherlink_recurring_users
        """
        data = await fetch_non_time_series_data(conn, query, "active_wallets", ["time", "etherlink_unique_users"])
        return data, None
    except Exception as e:
        return None, e

async def fetch_tzkt_recurring_users(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select * from mv_tzkt_recurring_users
        """
        data = await fetch_non_time_series_data(conn, query, "active_wallets", ["time", "tzkt_unique_users"])
        return data, None
    except Exception as e:
        return None, e

async def fetch_etherlink_plus_one_users(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select * from mv_etherlink_plus_one
        """
        data = await fetch_non_time_series_data(conn, query, "active_wallets", ["time", "etherlink_unique_users"])
        return data, None
    except Exception as e:
        return None, e

async def fetch_tzkt_plus_one_users(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select * from mv_tzkt_plus_one
        """
        data = await fetch_non_time_series_data(conn, query, "active_wallets", ["time", "tzkt_unique_users"])
        return data, None
    except Exception as e:
        return None, e

async def fetch_top_projects_transactions(conn) -> Tuple[List[Dict[str, Any]], Exception]:
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
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["Project", "Total Transactions", "Layer"])
        return data, None
    except Exception as e:
        return None, e

async def fetch_top_projects_users(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
            SELECT project, user_count, layer FROM mv_etherlink_projects_users
            where month = (SELECT MAX(month) FROM mv_etherlink_projects_users)
            and user_count > 0
            UNION ALL
            SELECT project, user_count, layer FROM mv_tzkt_projects_users
            where month = (SELECT MAX(month) FROM mv_tzkt_projects_users)
            and user_count > 0
            ORDER BY user_count DESC
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["Project", "Active Wallets", "Layer"])
        return data, None
    except Exception as e:
        return None, e

async def fetch_top_projects_tvl(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
      select project, round(sum(t.tvl))::INTEGER as tvl, 
		  CASE 
      		WHEN layer = 1 THEN 'Tezos L1'
      		WHEN layer = 2 THEN 'Etherlink'
  		  END AS layer 
		  FROM tvl t
		  join 
		  (select distinct project, layer from 
		  project_mappings) pm
		  ON SPLIT_PART(pm.project, ' ', 1) = SPLIT_PART(t.project_name, ' ', 1)
		  WHERE t.date = (
  		  	SELECT MAX(date)
  		  	FROM tvl
		  )
		  group by project, layer
		  order by tvl desc
        """
        data = await fetch_non_time_series_data(conn, query, "owner_project_tvl", ["Project", "TVL", "Layer"])
        return data, None
    except Exception as e:
        return None, e
