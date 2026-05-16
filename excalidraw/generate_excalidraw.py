import json
import os

def create_rect(id, x, y, width, height, text, bg_color="#e0eaff", border_style="solid", text_pos="center", text_color="#c92a2a"):
    rect_id = id
    text_id = id + "_text"
    
    rect = {
        "id": rect_id,
        "type": "rectangle",
        "x": x,
        "y": y,
        "width": width,
        "height": height,
        "angle": 0,
        "strokeColor": "#1e1e1e",
        "backgroundColor": bg_color,
        "fillStyle": "solid",
        "strokeWidth": 2,
        "strokeStyle": border_style,
        "roughness": 0,
        "opacity": 100,
        "groupIds": [],
        "roundness": {"type": 3},
        "boundElements": [{"id": text_id, "type": "text"}],
        "updated": 1,
        "link": None,
        "locked": False
    }
    
    ty = y + height/2 - 12
    tx = x + 10
    ta = "center"
    va = "middle"
    
    if text_pos == "top-left":
        ty = y + 10
        tx = x + 10
        ta = "left"
        va = "top"
        
    text_el = {
        "id": text_id,
        "type": "text",
        "x": tx,
        "y": ty,
        "width": width - 20,
        "height": 24,
        "angle": 0,
        "strokeColor": text_color,
        "backgroundColor": "transparent",
        "fillStyle": "solid",
        "strokeWidth": 1,
        "strokeStyle": "solid",
        "roughness": 0,
        "opacity": 100,
        "groupIds": [],
        "roundness": None,
        "updated": 1,
        "link": None,
        "locked": False,
        "text": text,
        "fontSize": 20,
        "fontFamily": 1,
        "textAlign": ta,
        "verticalAlign": va,
        "containerId": rect_id if text_pos == "center" else None
    }
    
    return [rect, text_el]

def create_arrow(id, start_el, end_el, label=None, text_color="#c92a2a"):
    s_cx = start_el['x'] + start_el['w'] / 2
    s_cy = start_el['y'] + start_el['h'] / 2
    e_cx = end_el['x'] + end_el['w'] / 2
    e_cy = end_el['y'] + end_el['h'] / 2
    
    dx = e_cx - s_cx
    dy = e_cy - s_cy
    
    if abs(dx) > abs(dy):
        if dx > 0: # right
            sx = start_el['x'] + start_el['w']
            sy = s_cy
            ex = end_el['x']
            ey = e_cy
        else: # left
            sx = start_el['x']
            sy = s_cy
            ex = end_el['x'] + end_el['w']
            ey = e_cy
    else:
        if dy > 0: # down
            sx = s_cx
            sy = start_el['y'] + start_el['h']
            ex = e_cx
            ey = end_el['y']
        else: # up
            sx = s_cx
            sy = start_el['y']
            ex = e_cx
            ey = end_el['y'] + end_el['h']
            
    arrow = {
        "id": id,
        "type": "arrow",
        "x": sx,
        "y": sy,
        "width": abs(ex - sx),
        "height": abs(ey - sy),
        "angle": 0,
        "strokeColor": "#000000",
        "backgroundColor": "transparent",
        "fillStyle": "solid",
        "strokeWidth": 2,
        "strokeStyle": "solid",
        "roughness": 0,
        "opacity": 100,
        "groupIds": [],
        "roundness": {"type": 2},
        "updated": 1,
        "link": None,
        "locked": False,
        "startBinding": {"elementId": start_el['id'], "gap": 2, "focus": 0},
        "endBinding": {"elementId": end_el['id'], "gap": 2, "focus": 0},
        "endArrowhead": "arrow",
        "startArrowhead": None,
        "points": [
            [0, 0],
            [ex - sx, ey - sy]
        ]
    }
    
    elements = [arrow]
    
    if label:
        text_id = id + "_text"
        mx = sx + (ex - sx) / 2
        my = sy + (ey - sy) / 2
        text_el = {
            "id": text_id,
            "type": "text",
            "x": mx - 50,
            "y": my - 12,
            "width": 100,
            "height": 24,
            "angle": 0,
            "strokeColor": text_color,
            "backgroundColor": "#ffffff",
            "fillStyle": "solid",
            "strokeWidth": 1,
            "strokeStyle": "solid",
            "roughness": 0,
            "opacity": 100,
            "groupIds": [],
            "roundness": None,
            "updated": 1,
            "link": None,
            "locked": False,
            "text": label,
            "fontSize": 14,
            "fontFamily": 1,
            "textAlign": "center",
            "verticalAlign": "middle"
        }
        elements.append(text_el)
        
    return elements

nodes = [
    # Tools/Components
    {"id": "local", "x": 50, "y": -50, "w": 200, "h": 60, "text": "🧰 Makefile & uv", "bg": "#f8f9fa"},
    {"id": "tiki", "x": 50, "y": 100, "w": 180, "h": 80, "text": "🌐 Tiki API", "bg": "#fff3bf"},
    
    # Docker Container
    {"id": "docker", "x": 300, "y": -10, "w": 1080, "h": 500, "text": "🐳 Docker Compose Network", "bg": "transparent", "border": "dashed", "text_pos": "top-left"},
    
    {"id": "crawler", "x": 380, "y": 100, "w": 200, "h": 80, "text": "🐍 Python Crawler", "bg": "#e0eaff"},
    {"id": "minio", "x": 680, "y": 100, "w": 200, "h": 80, "text": "🪣 MinIO\n(Raw & Parquet)", "bg": "#ffe3e3"},
    {"id": "dbt", "x": 1050, "y": 100, "w": 200, "h": 80, "text": "🦆 dbt + DuckDB\nTransform", "bg": "#e0eaff"},
    
    {"id": "airflow", "x": 380, "y": 250, "w": 200, "h": 80, "text": "⚙️ Apache Airflow", "bg": "#e0eaff"},
    {"id": "file_meta", "x": 680, "y": 250, "w": 200, "h": 80, "text": "🐝 File Metastore\n(Hive Catalog)", "bg": "#fff3bf"},
    {"id": "iceberg", "x": 1150, "y": 250, "w": 180, "h": 80, "text": "🧊 Apache Iceberg", "bg": "#d8f5a2"},
    
    {"id": "postgres", "x": 1050, "y": 250, "w": 200, "h": 60, "text": "🐘 PostgreSQL", "bg": "#e0eaff"},
    
    {"id": "trino", "x": 680, "y": 380, "w": 200, "h": 80, "text": "🐇 Trino Engine", "bg": "#eebefa"},
    {"id": "superset", "x": 1050, "y": 380, "w": 200, "h": 80, "text": "📊 Apache Superset", "bg": "#d3f9d8"}
]

edges = [
    {"id": "e_make1", "from": "local", "to": "airflow", "label": "Deploy"},
    {"id": "e_make2", "from": "local", "to": "docker", "label": "Start"},
    
    {"id": "e1", "from": "tiki", "to": "crawler", "label": "Fetch JSON"},
    {"id": "e2", "from": "crawler", "to": "minio", "label": "Save Raw"},
    {"id": "e3", "from": "minio", "to": "dbt", "label": "Read Raw"},
    {"id": "e4", "from": "dbt", "to": "iceberg", "label": "Write Format"},
    {"id": "e5", "from": "iceberg", "to": "file_meta", "label": "Catalog"},
    {"id": "e6", "from": "file_meta", "to": "trino", "label": "Metadata"},
    {"id": "e7", "from": "minio", "to": "trino", "label": "S3 Data"},
    
    {"id": "e8", "from": "postgres", "to": "superset", "label": "Backend DB"},
    {"id": "e9", "from": "trino", "to": "superset", "label": "SQL Queries"},
    
    {"id": "e10", "from": "airflow", "to": "crawler", "label": "Trigger"},
    {"id": "e11", "from": "airflow", "to": "dbt", "label": "Trigger"}
]

elements = []

node_dict = {}
for n in nodes:
    node_dict[n['id']] = n
    elements.extend(create_rect(
        n['id'], n['x'], n['y'], n['w'], n['h'], n['text'], 
        bg_color=n.get('bg', "#e0eaff"), 
        border_style=n.get('border', 'solid'),
        text_pos=n.get('text_pos', 'center'),
        text_color="#c92a2a"
    ))

for e in edges:
    start = node_dict[e['from']]
    end = node_dict[e['to']]
    elements.extend(create_arrow(e['id'], start, end, e['label'], text_color="#c92a2a"))

excalidraw = {
    "type": "excalidraw",
    "version": 2,
    "source": "https://excalidraw.com",
    "elements": elements,
    "appState": {
        "viewBackgroundColor": "#ffffff",
        "gridSize": None
    },
    "files": {}
}

out_path = '/home/tunguyenn99/my-project/tiki-lakehouse/excalidraw/architecture.excalidraw'
with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(excalidraw, f, indent=2, ensure_ascii=False)

print(f"Excalidraw file updated with File Metastore at {out_path}")
