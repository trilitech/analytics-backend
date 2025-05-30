from database.db_utils import get_db_conn
from database.defi_db import fetch_borrow_supply, fetch_borrow_supply_cap, fetch_max_slippage, fetch_slippage
from database.general_db import fetch_bot_transactions, fetch_bot_users, fetch_etherlink_plus_one_users, fetch_etherlink_recurring_users, fetch_etherlink_transactions, fetch_etherlink_users, fetch_top_projects_transactions, fetch_top_projects_tvl, fetch_top_projects_users, fetch_total_tvl_etherlink, fetch_total_tvl_tezos, fetch_total_tvl_tf, fetch_tzkt_plus_one_users, fetch_tzkt_recurring_users, fetch_tzkt_transactions, fetch_tzkt_users
from database.team_db import fetch_team_etherlink_transactions, fetch_team_etherlink_users, fetch_team_goals_users, fetch_team_predicted_users, fetch_team_projects_transactions, fetch_team_projects_transactions_split, fetch_team_projects_users, fetch_team_projects_users_split, fetch_team_tzkt_transactions, fetch_team_tzkt_users, fetch_team_actual_users
from database.individual_db import fetch_owner_actual_tvl, fetch_owner_predicted_tvl, fetch_owner_goals_tvl, fetch_owner_projects_tvl_split_chain, fetch_owner_top_projects_tvl, fetch_owner_top_projects_users, fetch_owner_actual_users, fetch_owner_goals_users, fetch_owner_predicted_users
from database.report_db import fetch_targets
from fastapi import Depends, Query, APIRouter

stats_router = APIRouter(prefix="/v1/stats")

@stats_router.get("/team_goals_users")
async def get_team_goals_users(department: str = Query(..., description="Department name"), conn=Depends(get_db_conn)):
    data, err = await fetch_team_goals_users(department, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_actual_users")
async def get_team_actual_users(department: str = Query(..., description="Department name"), conn=Depends(get_db_conn)):
    data, err = await fetch_team_actual_users(department, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_predicted_users")
async def get_team_predicted_users(department: str = Query(..., description="Department name"), conn=Depends(get_db_conn)):
    data, err = await fetch_team_predicted_users(department, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_transactions")
async def get_team_tzkt_transactions(department: str = Query(..., description="Department name"), conn=Depends(get_db_conn)):
    data, err = await fetch_team_tzkt_transactions(department, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_users")
async def get_team_tzkt_users(department: str = Query(..., description="Department name"), conn=Depends(get_db_conn)):
    data, err = await fetch_team_tzkt_users(department, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_transactions_etherlink")
async def get_team_etherlink_transactions(department: str = Query(..., description="Department name"), conn=Depends(get_db_conn)):
    data, err = await fetch_team_etherlink_transactions(department, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_users_etherlink")
async def get_team_etherlink_users(department: str = Query(..., description="Department name"), conn=Depends(get_db_conn)):
    data, err = await fetch_team_etherlink_users(department, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_projects_transactions")
async def get_team_projects_transactions(department: str = Query(..., description="Department name"), conn=Depends(get_db_conn)):
    data, err = await fetch_team_projects_transactions(department, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_projects_users")
async def get_team_projects_users(department: str = Query(..., description="Department name"), conn=Depends(get_db_conn)):
    data, err = await fetch_team_projects_users(department, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_projects_transactions_monthly")
async def get_team_projects_transactions_split(department: str = Query(..., description="Department name"), conn=Depends(get_db_conn)):
    data, err = await fetch_team_projects_transactions_split(department, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_projects_users_monthly")
async def get_team_projects_users_split(department: str = Query(..., description="Department name"), conn=Depends(get_db_conn)):
    data, err = await fetch_team_projects_users_split(department, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_actual_users")
async def get_owner_actual_users(owner: str = Query(..., description="Owner name"), conn=Depends(get_db_conn)):
    data, err = await fetch_owner_actual_users(owner, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_goals_users")
async def get_owner_goals_users(owner: str = Query(..., description="Owner name"), conn=Depends(get_db_conn)):
    data, err = await fetch_owner_goals_users(owner, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_predicted_users")
async def get_owner_predicted_users(owner: str = Query(..., description="Owner name"), conn=Depends(get_db_conn)):
    data, err = await fetch_owner_predicted_users(owner, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_top_projects")
async def get_top_projects_users_owner(owner: str = Query(..., description="Owner name"), conn=Depends(get_db_conn)):
    data, err = await fetch_owner_top_projects_users(owner, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_actual_tvl")
async def get_owner_actual_tvl(owner: str = Query(..., description="Owner name"), conn=Depends(get_db_conn)):
    data, err = await fetch_owner_actual_tvl(owner, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_predicted_tvl")
async def get_owner_predicted_tvl(owner: str = Query(..., description="Owner name"), conn=Depends(get_db_conn)):
    data, err = await fetch_owner_predicted_tvl(owner, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_goals_tvl")
async def get_owner_goals_tvl(owner: str = Query(..., description="Owner name"), conn=Depends(get_db_conn)):
    data, err = await fetch_owner_goals_tvl(owner, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_tvl_split")
async def get_owner_tvl_split(owner: str = Query(..., description="Owner name"), conn=Depends(get_db_conn)):
    data, err = await fetch_owner_top_projects_tvl(owner, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_tvl_split_chain")
async def get_owner_tvl_split_chain(owner: str = Query(..., description="Owner name"), conn=Depends(get_db_conn)):
    data, err = await fetch_owner_projects_tvl_split_chain(owner, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/etherlink_transactions")
async def get_etherlink_transactions(conn=Depends(get_db_conn)):
    data, err = await fetch_etherlink_transactions(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/tzkt_transactions")
async def get_tzkt_transactions(conn=Depends(get_db_conn)):
    data, err = await fetch_tzkt_transactions(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/bot_transactions")
async def get_bot_transactions(conn=Depends(get_db_conn)):
    data, err = await fetch_bot_transactions(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/etherlink_users")
async def get_etherlink_users(conn=Depends(get_db_conn)):
    data, err = await fetch_etherlink_users(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/bot_users")
async def get_bot_users(conn=Depends(get_db_conn)):
    data, err = await fetch_bot_users(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/tzkt_users")
async def get_tzkt_users(conn=Depends(get_db_conn)):
    data, err = await fetch_tzkt_users(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/etherlink_plus_one")
async def get_etherlink_plus_one_users(conn=Depends(get_db_conn)):
    data, err = await fetch_etherlink_plus_one_users(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/tzkt_plus_one")
async def get_tzkt_plus_one_users(conn=Depends(get_db_conn)):
    data, err = await fetch_tzkt_plus_one_users(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/etherlink_recurring_users")
async def get_etherlink_recurring_users(conn=Depends(get_db_conn)):
    data, err = await fetch_etherlink_recurring_users(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/tzkt_recurring_users")
async def get_tzkt_recurring_users(conn=Depends(get_db_conn)):
    data, err = await fetch_tzkt_recurring_users(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/transactions_group_by_project")
async def get_top_projects_transactions(conn=Depends(get_db_conn)):
    data, err = await fetch_top_projects_transactions(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/user_group_by_project")
async def get_top_projects_users(conn=Depends(get_db_conn)):
    data, err = await fetch_top_projects_users(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/tvl_group_by_project")
async def get_top_projects_tvl(conn=Depends(get_db_conn)):
    data, err = await fetch_top_projects_tvl(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/etherlink_tvl")
async def fetch_etherlink_tvl(conn=Depends(get_db_conn)):
    data, err = await fetch_total_tvl_etherlink(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/tezos_tvl")
async def fetch_tezos_tvl(conn=Depends(get_db_conn)):
    data, err = await fetch_total_tvl_tezos(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/tf_tvl")
async def fetch_tf_tvl(conn=Depends(get_db_conn)):
    data, err = await fetch_total_tvl_tf(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/slippage")
async def get_slippage(conn=Depends(get_db_conn)):
    data, err = await fetch_slippage(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/max_slippage")
async def get_max_slippage(conn=Depends(get_db_conn)):
    data, err = await fetch_max_slippage(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/supply_borrow")
async def get_supply_borrow(token: str = Query(..., description="Token"), conn=Depends(get_db_conn)):
    data, err = await fetch_borrow_supply(token, conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/supply_borrow_cap")
async def get_supply_borrow_cap(conn=Depends(get_db_conn)):
    data, err = await fetch_borrow_supply_cap(conn)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/report_targets")
async def get_report_targets(conn=Depends(get_db_conn)):
    data, err = await fetch_targets(conn)
    if err:
        return {"error": str(err)}
    return data
