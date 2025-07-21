from ninja import NinjaAPI
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

api = NinjaAPI()
@api.get("/hello")
def hello(request): 
    return "Hello world --"

@api.get("/test-db")
def test_db_connection(request):
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.environ["DB_HOST"],
            port="5432"
        )
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        conn.close()
        return {"success": True, "result": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@api.post("/assign-fleets")
def assign_fleets(request):

    # 如果用 Supabase，要連線資料庫
    conn = psycopg2.connect(
        dbname="postgres",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port="5432"
    )
    cur = conn.cursor()

    # 1. 找出 pending request
    cur.execute("""
        SELECT id, origin, destination, skill
        FROM patient_rq
        WHERE status = 'Pending'
    """)
    requests = cur.fetchall()

    # 2. 找出 available fleet
    cur.execute("""
        SELECT id, skill
        FROM patient_fleet
        WHERE id NOT IN (
            SELECT assigned_fleet_id
            FROM patient_rq
            WHERE status = 'Start'
            AND assigned_fleet_id IS NOT NULL
        )
    """)
    fleets = cur.fetchall()

    assigned = []
    for req in requests:
        req_id, origin, dest, skill_needed = req

        for i, fleet in enumerate(fleets):
            fleet_id, fleet_skill = fleet

            if fleet_skill == skill_needed:
                # assign fleet
                cur.execute("""
                    UPDATE patient_rq
                    SET assigned_fleet_id = %s, status = 'Scheduling'
                    WHERE id = %s
                """, (fleet_id, req_id))
                assigned.append({
                    "request_id": req_id,
                    "fleet_id": fleet_id
                })
                fleets.pop(i)  # remove from available
                break

    conn.commit()
    cur.close()
    conn.close()

    return {"assigned": assigned, "count": len(assigned)}