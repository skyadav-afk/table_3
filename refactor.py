import re
import os

files = ["daily_weekly1.py", "volume1.py", "sudden.py", "drift.py"]

for f in files:
    if not os.path.exists(f): continue
    
    with open(f, 'r') as file:
        content = file.read()
    
    # 1. get_baseline signatures
    content = re.sub(
        r'def get_baseline\((.+?), app', 
        r'def get_baseline(\1, proj, app', 
        content
    )
    content = re.sub(
        r'def get_baseline_30d\((.+?), app', 
        r'def get_baseline_30d(\1, proj, app', 
        content
    )
    content = re.sub(
        r'def median_delta\((.+?), app', 
        r'def median_delta(\1, proj, app', 
        content
    )

    # 2. DataFrame filtering inside get_baseline / get_baseline_30d / median_delta
    # (df.application_id == app) -> (df.project_id == proj) & (df.application_id == app)
    def repl_filter(m):
        prefix = m.group(1) # e.g. "baseline_df"
        indent = m.group(2) # indentation spaces before the line
        return f"{indent}({prefix}.project_id == proj) &\n{indent}({prefix}.application_id == app) &"
    
    content = re.sub(r'([ \t]*)\((\w+)\.application_id == app\) &', repl_filter, content)

    # 3. groupby lists
    content = content.replace(
        '["application_id", "service", "metric", "hour"]',
        '["project_id", "application_id", "service", "metric", "hour"]'
    )
    content = content.replace(
        '["application_id", "service", "metric", "day_of_week", "hour"]',
        '["project_id", "application_id", "service", "metric", "day_of_week", "hour"]'
    )
    content = content.replace(
        '["application_id", "service", "metric"]',
        '["project_id", "application_id", "service", "metric"]'
    )

    # 4. Unpacking group_key
    # for (app, svc, metric), group in grouped:
    content = content.replace(
        'for (app, svc, metric), group in grouped:',
        'for (proj, app, svc, metric), group in grouped:'
    )
    # daily_weekly1.py unpacking:
    content = content.replace(
        'app, svc, metric, hour = group_key',
        'proj, app, svc, metric, hour = group_key'
    )
    content = content.replace(
        'app, svc, metric, dow, hour = group_key',
        'proj, app, svc, metric, dow, hour = group_key'
    )

    # 5. get_baseline calls
    content = content.replace(
        'get_baseline(baseline_df, app, svc, metric)',
        'get_baseline(baseline_df, proj, app, svc, metric)'
    )
    content = content.replace(
        'get_baseline_30d(baseline_30d_df, app, svc, metric)',
        'get_baseline_30d(baseline_30d_df, proj, app, svc, metric)'
    )
    content = content.replace(
        'median_delta(hourly_df, app, svc, metric',
        'median_delta(hourly_df, proj, app, svc, metric'
    )

    # 6. promoted.append
    content = content.replace(
        '"application_id": app,',
        '"project_id": proj,\n            "application_id": app,'
    )

    with open(f, 'w') as file:
        file.write(content)

print("Replacement complete")
