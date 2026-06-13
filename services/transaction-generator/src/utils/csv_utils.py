import csv
import os

def write_csv(rows: list[dict], 
              path: str, 
              fieldnames: list[str]) -> None:
    
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        
    print(f"[CSV] Saved {len(rows):,} rows → {path}")
