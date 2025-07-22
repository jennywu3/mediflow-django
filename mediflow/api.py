from ninja import NinjaAPI
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

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

# assign_patient: only assign patient_rq
@api.post("/assign-patient")
def assign_patient(request):

    now_minutes = datetime.now().hour * 60 + datetime.now().minute
    conn = psycopg2.connect(
        dbname="postgres",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT id, skill
        FROM patient_rq
        WHERE status = 'Pending'
    """)
    requests = cur.fetchall()

    assigned = []

    for req_id, skill in requests:
        cur.execute("""
            SELECT pf.id AS fleet_id, pf."userId", pf.cost
            FROM patient_fleet pf
            WHERE pf.skill = %s
              AND %s BETWEEN pf.s_start AND pf.s_end
              AND pf."userId" NOT IN (
                  SELECT assigned_user_id FROM patient_rq WHERE status IN ('Scheduling', 'Start')
                  UNION
                  SELECT assigned_user_id FROM delivery_status WHERE "DeliveryStatus" IN ('Scheduling', 'Start')
              )
            ORDER BY pf.cost ASC
            LIMIT 1
        """, (skill, now_minutes))
        row = cur.fetchone()

        if row:
            fleet_id, user_id, _ = row
            cur.execute("""
                UPDATE patient_rq
                SET assigned_user_id = %s,
                    assigned_fleet_id = %s,
                    status = 'Scheduling'
                WHERE id = %s
            """, (user_id, fleet_id, req_id))

            assigned.append({"rq_id": req_id, "user_id": user_id, "fleet_id": fleet_id})

    conn.commit()
    cur.close()
    conn.close()
    return {"assigned": assigned, "count": len(assigned)}


# assign_material: only assign delivery_status
@api.post("/assign-material")
def assign_material(request):

    conn = psycopg2.connect(
        dbname="postgres",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port="5432"
    )
    cur = conn.cursor()

    cur.execute('SELECT "Item", "Category" FROM "Inventory"')
    inventory_map = dict(cur.fetchall())

    cur.execute("""
        SELECT id, "Item", "RequestTime"
        FROM delivery_status
        WHERE "DeliveryStatus" = 'Pending'
    """)
    pending_tasks = cur.fetchall()

    cur.execute("""
        SELECT id, "userId", type, cost, s_start, s_end
        FROM patient_fleet
        ORDER BY cost ASC
    """)
    all_fleets = cur.fetchall()

    assigned = []

    for delivery_id, item_name, request_time_str in pending_tasks:
        category = inventory_map.get(item_name)
        required_type = 2 if category == "Laundry" else 1

        try:
            dt = datetime.strptime(request_time_str, "%d/%m/%Y, %H:%M:%S")
            request_min = dt.hour * 60 + dt.minute
        except:
            continue

        for i, (fleet_id, user_id, f_type, cost, s_start, s_end) in enumerate(all_fleets):
            if int(f_type) != required_type or s_start is None or s_end is None:
                continue
            if not (s_start <= request_min <= s_end):
                continue

            cur.execute("""
                SELECT 1 FROM patient_rq WHERE assigned_user_id = %s AND status IN ('Scheduling', 'Start')
                UNION
                SELECT 1 FROM delivery_status WHERE assigned_user_id = %s AND "DeliveryStatus" IN ('Scheduling', 'Start')
            """, (user_id, user_id))
            if cur.fetchone():
                continue

            cur.execute("""
                UPDATE delivery_status
                SET assigned_user_id = %s,
                    assigned_fleet_id = %s,
                    "DeliveryStatus" = 'Scheduling'
                WHERE id = %s
            """, (user_id, fleet_id, delivery_id))

            assigned.append({"delivery_id": delivery_id, "item": item_name, "category": category,
                             "fleet_id": fleet_id, "user_id": user_id, "minute": request_min})
            all_fleets.pop(i)
            break

    conn.commit()
    cur.close()
    conn.close()
    return {"assigned": assigned, "count": len(assigned)}


# assign_all: assign both patient and material tasks
@api.post("/assign-all")
def assign_all(request):
    result1 = assign_patient(request)
    result2 = assign_material(request)
    return {
        "patient": result1,
        "material": result2,
        "total_assigned": result1["count"] + result2["count"]
    }