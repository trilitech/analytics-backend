from utils.data_processor import fetch_non_time_series_data
from typing import Tuple, List, Dict, Any
from database.db_utils import db_pool

async def fetch_slippage(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select amount, slippage, pool from defi_slippage order by amount
        """
        data = await fetch_non_time_series_data(conn, query, "slippage", ["amount", "slippage", "pool"])
        return data, None
    except Exception as e:
        return None, e

async def fetch_max_slippage(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select pool, amount, "limit" from defi_slippage_limits order by pool, amount
        """
        data = await fetch_non_time_series_data(conn, query, "slippage", ["pool", "amount", "limit"])
        return data, None
    except Exception as e:
        return None, e

async def fetch_borrow_supply(token: str, conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          select timestamp, total_supply, total_borrow from defi_borrow_supply where token = $1
		  order by timestamp asc
        """
        data = await fetch_non_time_series_data(conn, query, "slippage", ["time", "total_supply", "total_borrow"], token)
        return data, None
    except Exception as e:
        return None, e

async def fetch_borrow_supply_cap(conn) -> Tuple[List[Dict[str, Any]], Exception]:
    try:
        query = """
          SELECT 
          UPPER(d.token) AS token, 
          total_supply * p.price_usd AS total_supply, 
          total_borrow * p.price_usd AS total_borrow,
          supply_cap * p.price_usd AS supply_cap, 
          borrow_cap * p.price_usd AS borrow_cap
          FROM 
            defi_borrow_supply d
          JOIN 
            prices p 
          ON d.token = p.token AND d.timestamp = p.date
          WHERE 
            d.timestamp = (
                SELECT MAX(dbs.timestamp)
                FROM defi_borrow_supply dbs
                WHERE dbs.token = d.token AND DATE(dbs.timestamp) != CURRENT_DATE
            );
        """
        data = await fetch_non_time_series_data(conn, query, "supply", ["time", "total_supply", "total_borrow", "supply_cap", "borrow_cap"])
        return data, None
    except Exception as e:
        return None, e
