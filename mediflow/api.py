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

@api.post("/assign-patient")
def assign_patient(request):
    from datetime import datetime
    import psycopg2
    import os

    now_minutes = datetime.now().hour * 60 + datetime.now().minute

    conn = psycopg2.connect(
        dbname="postgres",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port="5432"
    )
    cur = conn.cursor()

    assigned = []

    try:
        # 1. 找出所有 Pending 任務
        cur.execute("""
            SELECT id, skill
            FROM patient_rq
            WHERE status = 'Pending'
        """)
        requests = cur.fetchall()
        print(f"[INFO] Found {len(requests)} pending patient tasks.")

        # 2. 預先撈出所有 fleet（依 cost 排序）
        cur.execute("""
            SELECT id, "userId", skill, cost, s_start, s_end
            FROM patient_fleet
            ORDER BY cost ASC
        """)
        all_fleets = cur.fetchall()
        print(f"[INFO] Found {len(all_fleets)} total fleets.")

        for req_id, required_skill in requests:
            print(f"\n[Task] Patient RQ ID={req_id}, Required Skill={required_skill}")

            for i, (fleet_id, user_id, skill, cost, s_start, s_end) in enumerate(all_fleets):
                print(f"  > Checking fleet_id={fleet_id}, skill={skill}, cost={cost}, time={s_start}-{s_end}")

                if skill != required_skill:
                    print("    ✘ Skill mismatch")
                    continue

                if s_start is None or s_end is None:
                    print("    ✘ Invalid working time")
                    continue

                if not (s_start <= now_minutes <= s_end):
                    print("    ✘ Not available at current time")
                    continue

                # 檢查 user 是否在其他任務中
                cur.execute("""
                    SELECT 1 FROM patient_rq WHERE assigned_user_id = %s AND status IN ('Scheduling', 'Start')
                    UNION
                    SELECT 1 FROM delivery_status WHERE assigned_user_id = %s AND "DeliveryStatus" IN ('Scheduling', 'Start')
                """, (user_id, user_id))

                if cur.fetchone():
                    print("    ✘ User already assigned to another task")
                    continue

                # ✔ Assign 成功
                cur.execute("""
                    UPDATE patient_rq
                    SET assigned_user_id = %s,
                        assigned_fleet_id = %s,
                        status = 'Scheduling'
                    WHERE id = %s
                """, (user_id, fleet_id, req_id))

                assigned.append({
                    "rq_id": req_id,
                    "user_id": user_id,
                    "fleet_id": fleet_id
                })

                print(f"[ASSIGNED] → Fleet {fleet_id} (User {user_id}) assigned to Patient RQ {req_id}")

                all_fleets.pop(i)  # 移除已分配的 fleet
                break  # 一筆任務只指派一個 fleet

        conn.commit()
        return {"assigned": assigned, "count": len(assigned)}

    except Exception as e:
        print(f"[ERROR] {e}")
        return {"error": str(e)}

    finally:
        cur.close()
        conn.close()



        
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