"""
Streamlist - Pure Python CLI app
Manage your list from the terminal.
"""
from datetime import datetime
from pathlib import Path

DATA_FILE = Path(__file__).parent / "streamlist_data.txt"


def load_items():
    """Load items from file."""
    items = []
    if DATA_FILE.exists():
        for line in DATA_FILE.read_text(encoding="utf-8").strip().split("\n"):
            if line:
                parts = line.split(" | ", 2)
                if len(parts) >= 3:
                    items.append({"id": int(parts[0]), "name": parts[1], "status": parts[2]})
    return items


def save_items(items):
    """Save items to file."""
    lines = [f"{i['id']} | {i['name']} | {i['status']}" for i in items]
    DATA_FILE.write_text("\n".join(lines), encoding="utf-8")


def show_list(items):
    """Display the list."""
    if not items:
        print("  (empty)")
        return
    for i in items:
        print(f"  [{i['id']}] {i['name']} — {i['status']}")


def main():
    items = load_items()
    next_id = max((i["id"] for i in items), default=0) + 1

    print("=" * 50)
    print("  STREAMLIST - Python CLI")
    print("  add | list | complete <id> | remove <id> | quit")
    print("=" * 50)

    while True:
        try:
            cmd = input("\n> ").strip().lower().split(maxsplit=1)
            action = cmd[0] if cmd else ""
            arg = cmd[1] if len(cmd) > 1 else ""

            if action == "quit" or action == "q":
                print("Bye!")
                break
            elif action == "list" or action == "ls":
                show_list(items)
            elif action == "add":
                name = arg or input("  Item name: ").strip()
                if name:
                    items.append({"id": next_id, "name": name, "status": "Active"})
                    next_id += 1
                    save_items(items)
                    print(f"  Added: {name}")
            elif action == "complete":
                try:
                    idx = int(arg)
                    for i in items:
                        if i["id"] == idx:
                            i["status"] = "Completed"
                            save_items(items)
                            print(f"  Completed: {i['name']}")
                            break
                    else:
                        print("  ID not found")
                except ValueError:
                    print("  Usage: complete <id>")
            elif action == "remove":
                try:
                    idx = int(arg)
                    items = [i for i in items if i["id"] != idx]
                    save_items(items)
                    print("  Removed")
                except ValueError:
                    print("  Usage: remove <id>")
            elif action:
                print("  Unknown command. Try: add, list, complete <id>, remove <id>, quit")
        except KeyboardInterrupt:
            print("\nBye!")
            break


if __name__ == "__main__":
    main()
