from ninja import NinjaAPI
import os
import psycopg2
import psycopg2.extras as extras
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
api = NinjaAPI()

def get_conn():
    return psycopg2.connect(
        dbname="postgres",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port="5432"
    )

@api.get("/hello")
def hello(request):
    return "Hello world --"

@api.get("/test-db")
def test_db_connection(request):
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1;")
            return {"success": True, "result": cur.fetchone()}
    except Exception as e:
        return {"success": False, "error": str(e)}

# -----------------------
# Patient assignment
# -----------------------
@api.post("/assign-patient")
def assign_patient(request):
    now_minutes = datetime.now().hour * 60 + datetime.now().minute
    assigned = []

    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                # Pending patient tasks (Priority desc, then id asc)
                cur.execute("""
                    SELECT id, skill
                    FROM patient_rq
                    WHERE status = 'Pending'
                    ORDER BY "Priority" DESC, id ASC
                """)
                requests = cur.fetchall()
                if not requests:
                    return {"assigned": [], "count": 0}

                # Fleets available now & not already busy; cheapest first
                cur.execute("""
                    SELECT pf.id        AS fleet_id,
                           pf."userId"  AS user_id,
                           pf.skill,
                           pf.cost
                    FROM patient_fleet pf
                    WHERE pf.s_start IS NOT NULL
                      AND pf.s_end   IS NOT NULL
                      AND pf.s_start <= %s
                      AND %s <= pf.s_end
                      AND EXISTS (
                           SELECT 1
                           FROM patient_rq pr
                           WHERE pr.status = 'Pending'
                             AND pr.skill  = pf.skill
                      )
                      AND NOT EXISTS (
                           SELECT 1 FROM patient_rq x
                           WHERE x.assigned_user_id = pf."userId"
                             AND x.status IN ('Scheduling','Start')
                      )
                      AND NOT EXISTS (
                           SELECT 1 FROM delivery_status y
                           WHERE y.assigned_user_id = pf."userId"
                             AND y."DeliveryStatus" IN ('Scheduling','Start')
                      )
                    ORDER BY pf.cost ASC
                """, (now_minutes, now_minutes))
                fleets = cur.fetchall()
                if not fleets:
                    return {"assigned": [], "count": 0}

                from collections import defaultdict, deque
                fleets_by_skill = defaultdict(deque)
                for f in fleets:
                    fleets_by_skill[f["skill"]].append(f)

                used_users = set()
                patient_updates = []  # (user_id, fleet_id, rq_id)

                for req in requests:
                    dq = fleets_by_skill.get(req["skill"])
                    if not dq:
                        continue
                    while dq:
                        f = dq[0]
                        if f["user_id"] in used_users:
                            dq.popleft()
                            continue
                        used_users.add(f["user_id"])
                        patient_updates.append((f["user_id"], f["fleet_id"], req["id"]))
                        assigned.append({
                            "rq_id": req["id"],
                            "user_id": f["user_id"],
                            "fleet_id": f["fleet_id"],
                        })
                        dq.popleft()
                        break

                if patient_updates:
                    extras.execute_batch(
                        cur,
                        """
                        UPDATE patient_rq
                           SET assigned_user_id = %s,
                               assigned_fleet_id = %s,
                               status = 'Scheduling'
                         WHERE id = %s
                        """,
                        patient_updates,
                        page_size=200,
                    )

        return {"assigned": assigned, "count": len(assigned)}
    except Exception as e:
        return {"error": str(e)}

# -----------------------
# Material assignment
# -----------------------
@api.post("/assign-material")
def assign_material(request):
    assigned = []

    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                # Pending material tasks, order by Priority desc then id
                cur.execute("""
                    SELECT d.id,
                           d."Item",
                           EXTRACT(HOUR   FROM to_timestamp(d."RequestTime",'DD/MM/YYYY, HH24:MI:SS'))*60 +
                           EXTRACT(MINUTE FROM to_timestamp(d."RequestTime",'DD/MM/YYYY, HH24:MI:SS')) AS request_min
                    FROM delivery_status d
                    WHERE d."DeliveryStatus" = 'Pending'
                    ORDER BY d."Priority" DESC, d.id ASC
                """)
                pending = cur.fetchall()
                if not pending:
                    return {"assigned": [], "count": 0}

                # Map item -> category
                cur.execute('SELECT "Item","Category" FROM "Inventory"')
                inv_map = dict(cur.fetchall())

                # Fleet type is TEXT in schema -> use string "1"/"2"
                required_types = set()
                for row in pending:
                    cat = inv_map.get(row["Item"])
                    required_types.add("2" if cat == "Laundry" else "1")

                if not required_types:
                    return {"assigned": [], "count": 0}

                # Fetch fleets by required types (text[]), cheapest first
                cur.execute("""
                    SELECT pf.id       AS fleet_id,
                           pf."userId" AS user_id,
                           pf.type,              
                           pf.cost,
                           pf.s_start,
                           pf.s_end
                    FROM patient_fleet pf
                    WHERE pf.type = ANY(%s)            
                      AND pf.s_start IS NOT NULL
                      AND pf.s_end   IS NOT NULL
                      AND NOT EXISTS (
                           SELECT 1 FROM patient_rq x
                           WHERE x.assigned_user_id = pf."userId"
                             AND x.status IN ('Scheduling','Start')
                      )
                      AND NOT EXISTS (
                           SELECT 1 FROM delivery_status y
                           WHERE y.assigned_user_id = pf."userId"
                             AND y."DeliveryStatus" IN ('Scheduling','Start')
                      )
                    ORDER BY pf.cost ASC
                """, (list(required_types),))
                fleets = cur.fetchall()
                if not fleets:
                    return {"assigned": [], "count": 0}

                from collections import defaultdict, deque
                fleets_by_type = defaultdict(deque)
                for f in fleets:
                    # keep string keys ("1"/"2"), DO NOT cast to int
                    fleets_by_type[f["type"]].append(f)

                used_users = set()
                material_updates = []  # (user_id, fleet_id, delivery_id)

                for row in pending:
                    item   = row["Item"]
                    reqmin = int(row["request_min"] or 0)
                    cat    = inv_map.get(item)
                    rtype  = "2" if cat == "Laundry" else "1"
                    dq     = fleets_by_type.get(rtype)
                    if not dq:
                        continue

                    while dq:
                        f = dq[0]
                        if (f["user_id"] in used_users) or not (f["s_start"] <= reqmin <= f["s_end"]):
                            dq.popleft()
                            continue
                        used_users.add(f["user_id"])
                        material_updates.append((f["user_id"], f["fleet_id"], row["id"]))
                        assigned.append({
                            "delivery_id": row["id"],
                            "item": item,
                            "category": cat,
                            "fleet_id": f["fleet_id"],
                            "user_id": f["user_id"],
                            "minute": reqmin,
                        })
                        dq.popleft()
                        break

                if material_updates:
                    extras.execute_batch(
                        cur,
                        """
                        UPDATE delivery_status
                           SET assigned_user_id = %s,
                               assigned_fleet_id = %s,
                               "DeliveryStatus" = 'Scheduling'
                         WHERE id = %s
                        """,
                        material_updates,
                        page_size=200,
                    )

        return {"assigned": assigned, "count": len(assigned)}
    except Exception as e:
        return {"error": str(e)}

# -----------------------
# Assign both
# -----------------------
@api.post("/assign-all")
def assign_all(request):
    p = assign_patient(request)
    m = assign_material(request)
    pc = p.get("count", 0) if isinstance(p, dict) else 0
    mc = m.get("count", 0) if isinstance(m, dict) else 0
    return {"patient": p, "material": m, "total_assigned": pc + mc}