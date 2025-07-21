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


@api.post("/assign-staff")
def assign_staff(request):
    conn = psycopg2.connect(
        dbname="postgres",
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port="5432"
    )
    cur = conn.cursor()

    # 找出待指派（Pending）任務
    cur.execute("""
        SELECT id, skill
        FROM patient_rq
        WHERE status = 'Pending'
    """)
    requests = cur.fetchall()

    # 找出可用人員（不是已派送任務中、且是 Nurse 或 Delivery Dept.）
    cur.execute("""
        SELECT id, "UserDept"
        FROM "User"
        WHERE "UserDept" IN ('Nurse', 'Delivery Dept.')
          AND id NOT IN (
              SELECT assigned_user_id
              FROM patient_rq
              WHERE status IN ('Start', 'Scheduling') AND assigned_user_id IS NOT NULL
          )
    """)
    users = cur.fetchall()

    assigned = []
    for req_id, skill_needed in requests:
        for i, (user_id, user_dept) in enumerate(users):
            if (skill_needed == 'Nurse' and user_dept == 'Nurse') or \
               (skill_needed == 'Delivery Dept.' and user_dept == 'Delivery Dept.'):
                cur.execute("""
                    UPDATE patient_rq
                    SET assigned_user_id = %s, status = 'Scheduling'
                    WHERE id = %s
                """, (user_id, req_id))
                assigned.append({ "rq_id": req_id, "user_id": user_id })
                users.pop(i)  # 移除已使用的 user
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