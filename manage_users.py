# manage_users.py
import sqlite3
from core.auth import create_user, init_db

DB_PATH = "users.db"

def list_users():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("SELECT id, login, tariff, activated_at, is_active, hwid FROM users")
    rows = cur.fetchall()
    if not rows:
        print("Нет пользователей")
    else:
        print("ID | Логин | Тариф | Активирован | Активен | HWID")
        print("-" * 60)
        for row in rows:
            print(f"{row[0]:<3} | {row[1]:<10} | {row[2]:<6} | {row[3][:10]} | {row[4]:<7} | {row[5] or '—'}")
    conn.close()

def delete_user(login):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("DELETE FROM users WHERE login = ?", (login,))
    conn.commit()
    if cur.rowcount:
        print(f"✅ Пользователь {login} удалён")
    else:
        print(f"❌ Пользователь {login} не найден")
    conn.close()

def reset_hwid(login):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("UPDATE users SET hwid = NULL WHERE login = ?", (login,))
    conn.commit()
    if cur.rowcount:
        print(f"✅ HWID для {login} сброшен")
    else:
        print(f"❌ Пользователь {login} не найден")
    conn.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Управление пользователями:")
        print("  python manage_users.py list")
        print("  python manage_users.py add <login> <пароль> [тариф]")
        print("  python manage_users.py delete <login>")
        print("  python manage_users.py reset_hwid <login>")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "list":
        list_users()
    elif cmd == "add":
        login = sys.argv[2] if len(sys.argv) > 2 else None
        password = sys.argv[3] if len(sys.argv) > 3 else None
        tariff = sys.argv[4] if len(sys.argv) > 4 else "trial"
        if not login or not password:
            print("❌ Укажите логин и пароль")
            sys.exit(1)
        init_db()
        if create_user(login, password, tariff):
            print(f"✅ Пользователь {login} создан (тариф: {tariff})")
        else:
            print("❌ Ошибка: логин уже существует")
    elif cmd == "delete":
        login = sys.argv[2] if len(sys.argv) > 2 else None
        if not login:
            print("❌ Укажите логин")
            sys.exit(1)
        delete_user(login)
    elif cmd == "reset_hwid":
        login = sys.argv[2] if len(sys.argv) > 2 else None
        if not login:
            print("❌ Укажите логин")
            sys.exit(1)
        reset_hwid(login)
    else:
        print("❌ Неизвестная команда")