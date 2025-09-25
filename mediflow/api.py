from ninja import NinjaAPI
import os
import psycopg2
import psycopg2.extras as extras
from dotenv import load_dotenv
from datetime import datetime
from collections import defaultdict, deque

load_dotenv()

api = NinjaAPI()

def get_conn():
    """Get database connection"""
    return psycopg2.connect(
        dbname="postgres",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port="5432",
    )

@api.get("/hello")
def hello(request):
    return "Hello world --"

@api.get("/test-db")
def test_db_connection(request):
    """Test database connection"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1;")
            return {"success": True, "result": cur.fetchone()}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==============================================
# PATIENT ASSIGNMENT
# ==============================================
@api.post("/assign-patient")
def assign_patient(request):
    """
    Assign patients to available healthcare staff
    Priority: High > Medium > Low, then by ID order
    Match by skill and assign to lowest cost available staff
    """
    current_minutes = datetime.now().hour * 60 + datetime.now().minute
    assigned = []

    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                
                # Get pending patient requests (ordered by priority, then ID)
                # priority is numeric: 2=High, 1=Medium, 0=Low
                cur.execute("""
                    SELECT id, skill, priority
                    FROM patient_rq
                    WHERE status = 'Pending'
                    ORDER BY
                        CASE
                            WHEN priority = '2' THEN 3
                            WHEN priority = '1' THEN 2
                            WHEN priority = '0' THEN 1
                            ELSE 0
                        END DESC,
                        id ASC
                """)
                requests = cur.fetchall()
                
                if not requests:
                    return {"assigned": [], "count": 0}

                # Get available fleet members (on duty, not busy, ordered by cost)
                cur.execute("""
                    SELECT
                        pf.id AS fleet_id,
                        pf."userId" AS user_id,
                        pf.skill,
                        pf.cost
                    FROM patient_fleet pf
                    WHERE pf.s_start IS NOT NULL
                      AND pf.s_end IS NOT NULL
                      AND pf.s_start <= %s
                      AND %s <= pf.s_end
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
                """, (current_minutes, current_minutes))
                fleets = cur.fetchall()
                
                if not fleets:
                    return {"assigned": [], "count": 0}

                # Group fleet by skill for easier matching
                fleets_by_skill = defaultdict(deque)
                for f in fleets:
                    fleets_by_skill[f["skill"]].append(f)

                used_users = set()
                updates = []  # (user_id, fleet_id, request_id)

                # Assign each request to available fleet member
                for req in requests:
                    skill_queue = fleets_by_skill.get(req["skill"])
                    if not skill_queue:
                        continue
                        
                    # Find first available person with matching skill
                    while skill_queue:
                        fleet_member = skill_queue[0]
                        if fleet_member["user_id"] in used_users:
                            skill_queue.popleft()  # Remove unavailable person
                            continue
                            
                        # Assign this person
                        used_users.add(fleet_member["user_id"])
                        updates.append((fleet_member["user_id"], fleet_member["fleet_id"], req["id"]))
                        assigned.append({
                            "request_id": req["id"],
                            "user_id": fleet_member["user_id"],
                            "fleet_id": fleet_member["fleet_id"],
                            "skill": req["skill"],
                            "priority": req["priority"]
                        })
                        skill_queue.popleft()
                        break

                # Update database with assignments
                if updates:
                    extras.execute_batch(
                        cur,
                        """
                        UPDATE patient_rq
                           SET assigned_user_id = %s,
                               assigned_fleet_id = %s,
                               status = 'Scheduling'
                         WHERE id = %s
                        """,
                        updates,
                        page_size=200,
                    )

        return {"assigned": assigned, "count": len(assigned)}

    except Exception as e:
        return {"error": str(e)}

# ==============================================
# MATERIAL DELIVERY ASSIGNMENT
# ==============================================
@api.post("/assign-material")
def assign_material(request):
    """
    Assign material deliveries to available staff or AGV
    Laundry items need Type 2 AGV, others need Type 1
    Match delivery time with vehicle working hours
    """
    assigned = []

    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                
                # Get pending delivery requests with time converted to minutes
                # Priority is text: "High", "Medium", "Low" - order by priority then ID
                cur.execute("""
                    SELECT
                        d.id,
                        d."Item",
                        d."Priority",
                        EXTRACT(HOUR   FROM to_timestamp(d."RequestTime",'DD/MM/YYYY, HH24:MI:SS'))*60
                      + EXTRACT(MINUTE FROM to_timestamp(d."RequestTime",'DD/MM/YYYY, HH24:MI:SS')) AS request_min
                    FROM delivery_status d
                    WHERE d."DeliveryStatus" = 'Pending'
                    ORDER BY
                        CASE
                            WHEN d."Priority" ILIKE 'high'   THEN 3
                            WHEN d."Priority" ILIKE 'medium' THEN 2
                            WHEN d."Priority" ILIKE 'low'    THEN 1
                            ELSE 0
                        END DESC,
                        d.id ASC
                """)
                pending = cur.fetchall()
                
                if not pending:
                    return {"assigned": [], "count": 0}

                # Get item categories (to determine fleet type needed)
                cur.execute('SELECT "Item","Category" FROM "Inventory"')
                item_categories = dict(cur.fetchall())

                # Find what fleet types we need
                needed_types = set()
                for row in pending:
                    category = item_categories.get(row["Item"])
                    vehicle_type = '2' if category == "Laundry" else '1'
                    needed_types.add(vehicle_type)
                    
                if not needed_types:
                    return {"assigned": [], "count": 0}

                # Get available vehicles (not busy, correct type, ordered by cost)
                cur.execute("""
                    SELECT
                        pf.id AS fleet_id,
                        pf."userId" AS user_id,
                        pf.type,
                        pf.cost,
                        pf.s_start,
                        pf.s_end
                    FROM patient_fleet pf
                    WHERE pf.type = ANY(%s)
                      AND pf.s_start IS NOT NULL
                      AND pf.s_end IS NOT NULL
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
                """, (list(needed_types),))
                fleets = cur.fetchall()
                
                if not fleets:
                    return {"assigned": [], "count": 0}

                # Group vehicles by type
                fleets_by_type = defaultdict(deque)
                for f in fleets:
                    fleets_by_type[str(f["type"])].append(f)

                used_users = set()
                updates = []  # (user_id, fleet_id, delivery_id)

                # Assign each delivery to available vehicle
                for delivery in pending:
                    item = delivery["Item"]
                    request_minutes = int(delivery["request_min"] or 0)
                    category = item_categories.get(item)
                    vehicle_type = '2' if category == "Laundry" else '1'
                    
                    type_queue = fleets_by_type.get(vehicle_type)
                    if not type_queue:
                        continue

                    # Find available vehicle that can handle this delivery time
                    while type_queue:
                        vehicle = type_queue[0]
                        
                        # Check if vehicle is available and can work at request time
                        if (vehicle["user_id"] in used_users or 
                            not (vehicle["s_start"] <= request_minutes <= vehicle["s_end"])):
                            type_queue.popleft()
                            continue
                            
                        # Assign this vehicle
                        used_users.add(vehicle["user_id"])
                        updates.append((vehicle["user_id"], vehicle["fleet_id"], delivery["id"]))
                        assigned.append({
                            "delivery_id": delivery["id"],
                            "item": item,
                            "category": category,
                            "priority": delivery["Priority"],
                            "fleet_id": vehicle["fleet_id"],
                            "user_id": vehicle["user_id"],
                            "request_minutes": request_minutes,
                        })
                        type_queue.popleft()
                        break

                # Update database with assignments
                if updates:
                    extras.execute_batch(
                        cur,
                        """
                        UPDATE delivery_status
                           SET assigned_user_id = %s,
                               assigned_fleet_id = %s,
                               "DeliveryStatus" = 'Scheduling'
                         WHERE id = %s
                        """,
                        updates,
                        page_size=200,
                    )

        return {"assigned": assigned, "count": len(assigned)}

    except Exception as e:
        return {"error": str(e)}

# ==============================================
# ASSIGN BOTH PATIENTS AND MATERIALS
# ==============================================
@api.post("/assign-all")
def assign_all(request):
    """Assign both patients and materials in one call"""
    patient_result = assign_patient(request)
    material_result = assign_material(request)

    patient_count = patient_result.get("count", 0) if isinstance(patient_result, dict) else 0
    material_count = material_result.get("count", 0) if isinstance(material_result, dict) else 0
    
    return {
        "patient": patient_result, 
        "material": material_result, 
        "total_assigned": patient_count + material_count
    }