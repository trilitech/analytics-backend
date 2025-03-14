from database import fetch_owner_actual_tvl, fetch_owner_predicted_tvl, fetch_owner_goals_tvl, fetch_owner_top_projects_tvl, fetch_team_goals_users, fetch_owner_top_projects_users, fetch_owner_actual_users, fetch_owner_goals_users, fetch_owner_predicted_users, fetch_team_predicted_users, fetch_top_projects_transactions, fetch_team_actual_users
from fastapi import Query, APIRouter

stats_router = APIRouter(prefix="/v1/stats")

@stats_router.get("/team_goals_users")
async def get_team_goals_users(department: str = Query(..., description="Department name")):
    data, err = await fetch_team_goals_users(department)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_actual_users")
async def get_team_actual_users(department: str = Query(..., description="Department name")):
    data, err = await fetch_team_actual_users(department)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/team_predicted_users")
async def get_team_predicted_users(department: str = Query(..., description="Department name")):
    data, err = await fetch_team_predicted_users(department)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_actual_users")
async def get_owner_actual_users(owner: str = Query(..., description="Owner name")):
    data, err = await fetch_owner_actual_users(owner)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_goals_users")
async def get_owner_goals_users(owner: str = Query(..., description="Owner name")):
    data, err = await fetch_owner_goals_users(owner)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_predicted_users")
async def get_owner_predicted_users(owner: str = Query(..., description="Owner name")):
    data, err = await fetch_owner_predicted_users(owner)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_top_projects")
async def get_top_projects_users_owner(owner: str = Query(..., description="Owner name")):
    data, err = await fetch_owner_top_projects_users(owner)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_actual_tvl")
async def get_owner_actual_tvl(owner: str = Query(..., description="Owner name")):
    data, err = await fetch_owner_actual_tvl(owner)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_predicted_tvl")
async def get_owner_predicted_tvl(owner: str = Query(..., description="Owner name")):
    data, err = await fetch_owner_predicted_tvl(owner)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_goals_tvl")
async def get_owner_goals_tvl(owner: str = Query(..., description="Owner name")):
    data, err = await fetch_owner_goals_tvl(owner)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/owner_tvl_split")
async def get_owner_tvl_split(owner: str = Query(..., description="Owner name")):
    data, err = await fetch_owner_top_projects_tvl(owner)
    if err:
        return {"error": str(err)}
    return data

@stats_router.get("/transactions_group_by_project")
async def get_top_projects_transactions():
    data, err = await fetch_top_projects_transactions()
    if err:
        return {"error": str(err)}
    return data
