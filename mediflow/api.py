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
    import psycopg2
    import os
    from datetime import datetime

    now_minutes = datetime.now().hour * 60 + datetime.now().minute

    conn = psycopg2.connect(
        dbname="postgres",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port="5432"
    )
    cur = conn.cursor()

    # 1. 找出所有 Pending 任務
    cur.execute("""
        SELECT id, skill
        FROM patient_rq
        WHERE status = 'Pending'
    """)
    requests = cur.fetchall()

    assigned = []

    for req_id, skill in requests:
        # 2. 找出符合 skill 的 fleet，並時間可用
        cur.execute("""
            SELECT pf.id AS fleet_id, pf."userId", pf.cost
            FROM patient_fleet pf
            WHERE pf.skill = %s
              AND %s BETWEEN pf.s_start AND pf.s_end
              AND pf."userId" NOT IN (
                  SELECT assigned_user_id
                  FROM patient_rq
                  WHERE status IN ('Scheduling', 'Start')
                    AND assigned_user_id IS NOT NULL
              )
            ORDER BY pf.cost ASC
            LIMIT 1
        """, (skill, now_minutes))
        row = cur.fetchone()

        if row:
            fleet_id, user_id, _ = row
            # 更新任務：同時填 assigned_user_id 和 assigned_fleet_id
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

    conn.commit()
    cur.close()
    conn.close()

    return { "assigned": assigned, "count": len(assigned) }




@api.post("/assign-material")
def assign_material_staff(request):

    conn = psycopg2.connect(
        dbname="postgres",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port="5432"
    )
    cur = conn.cursor()

    # 查 inventory map: item -> category
    cur.execute('SELECT "Item", "Category" FROM "Inventory"')
    inventory_map = dict(cur.fetchall())

    # 查所有 Pending delivery 任務
    cur.execute("""
        SELECT id, "Item", "RequestTime"
        FROM delivery_status
        WHERE "DeliveryStatus" = 'Pending'
    """)
    pending_tasks = cur.fetchall()

    # 查所有 fleet（AGV + Transporter），依 cost 升冪排序
    cur.execute("""
        SELECT id, "userId", type, cost, s_start, s_end
        FROM patient_fleet
        ORDER BY cost ASC
    """)
    all_fleets = cur.fetchall()

    assigned = []

    for delivery_id, item_name, request_time_str in pending_tasks:
        # --- Step 1. 判斷類型是否為 Laundry ---
        category = inventory_map.get(item_name)
        required_type = 2 if category == "Laundry" else 1

        # --- Step 2. 將時間轉換為分鐘 ---
        try:
            dt = datetime.strptime(request_time_str, "%d/%m/%Y, %H:%M:%S")
            request_min = dt.hour * 60 + dt.minute
        except Exception as e:
            continue  # 時間格式錯誤 → 跳過

        # --- Step 3. 搜尋可用 fleet ---
        for i, (fleet_id, user_id, f_type, cost, s_start, s_end) in enumerate(all_fleets):
            if f_type != required_type:
                continue
            if s_start <= request_min <= s_end:
                # 可用 → 指派
                cur.execute("""
                    UPDATE delivery_status
                    SET assigned_user_id = %s,
                        assigned_fleet_id = %s,
                        "DeliveryStatus" = 'Scheduling'
                    WHERE id = %s
                """, (user_id, fleet_id, delivery_id))

                assigned.append({
                    "delivery_id": delivery_id,
                    "item": item_name,
                    "category": category,
                    "fleet_id": fleet_id,
                    "user_id": user_id,
                    "minute": request_min
                })
                all_fleets.pop(i)
                break  # 指派完成，換下一筆任務

    conn.commit()
    cur.close()
    conn.close()

    return {
        "assigned": assigned,
        "count": len(assigned)
    }


@api.post("/assign-staff")
def assign_staff(request):
    import psycopg2
    import os

    conn = psycopg2.connect(
        dbname="postgres",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port="5432"
    )
    cur = conn.cursor()

    # skill to user department 映射表（可擴充）
    skill_to_dept = {
        "nurse": "Nurse",
        "transporter": "Delivery Dept."
    }

    # 1. 找出 Pending 任務
    cur.execute("""
        SELECT id, skill
        FROM patient_rq
        WHERE status = 'Pending'
    """)
    requests = cur.fetchall()

    # 2. 找出可用人員（未指派中、符合部門）
    cur.execute("""
        SELECT id, "UserDept"
        FROM "User"
        WHERE "UserDept" IN ('Nurse', 'Delivery Dept.')
          AND id NOT IN (
              SELECT assigned_user_id
              FROM patient_rq
              WHERE status IN ('Start', 'Scheduling')
                AND assigned_user_id IS NOT NULL
          )
    """)
    users = cur.fetchall()

    assigned = []
    for req_id, skill in requests:
        expected_dept = skill_to_dept.get(skill.lower())  # skill 可能是 lowercase
        if not expected_dept:
            continue  # 無對應的部門，跳過

        for i, (user_id, user_dept) in enumerate(users):
            if user_dept == expected_dept:
                cur.execute("""
                    UPDATE patient_rq
                    SET assigned_user_id = %s, status = 'Scheduling'
                    WHERE id = %s
                """, (user_id, req_id))
                assigned.append({ "rq_id": req_id, "user_id": user_id })
                users.pop(i)  # 避免同一個人被重複使用
                break

    conn.commit()
    cur.close()
    conn.close()

    return {"assigned": assigned, "count": len(assigned)}



@api.post("/assign-fleets")
def assign_fleets(request):
    import os
    import psycopg2

    conn = psycopg2.connect(
        dbname="postgres",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port="5432"
    )
    cur = conn.cursor()

    # 1. 找出所有 Pending requests
    cur.execute("""
        SELECT id, origin, destination, skill
        FROM patient_rq
        WHERE status = 'Pending'
    """)
    requests = cur.fetchall()

    assigned = []
    for req in requests:
        req_id, origin, dest, skill_needed = req

        # 2. 動態查找這筆 request 最適合的 fleet
        cur.execute("""
            SELECT id
            FROM patient_fleet
            WHERE skill = %s
              AND id NOT IN (
                  SELECT assigned_fleet_id
                  FROM patient_rq
                  WHERE status IN ('Start', 'Scheduling')
                    AND assigned_fleet_id IS NOT NULL
              )
            ORDER BY cost ASC, available_time ASC
            LIMIT 1
        """, (skill_needed,))
        result = cur.fetchone()

        if result:
            fleet_id = result[0]

            # 3. 指派 fleet，並設為 Scheduling
            cur.execute("""
                UPDATE patient_rq
                SET assigned_fleet_id = %s, status = 'Scheduling'
                WHERE id = %s
            """, (fleet_id, req_id))

            assigned.append({
                "request_id": req_id,
                "fleet_id": fleet_id
            })

    conn.commit()
    cur.close()
    conn.close()

    return {"assigned": assigned, "count": len(assigned)}