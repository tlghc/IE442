#!/usr/bin/env python
# coding: utf-8

# In[5]:


import sqlite3
import pandas as pd

# Connect to SQLite database (creates 'MRP.db' if it doesn't exist)
db = sqlite3.connect("MRP.db")

# Enable foreign key enforcement in SQLite
db.execute("PRAGMA foreign_keys = 1;")

# Create a cursor object
cursor = db.cursor()


# Execute SQL commands to create a table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS mrp (
        part_id INTEGER,
        period_id INTEGER,
        gross_requirement INTEGER,
        scheduled_receipt INTEGER,
        projected_inventory INTEGER,
        net_requirement INTEGER,
        planned_order_receipt INTEGER,
        planned_order_release INTEGER,
        PRIMARY KEY (part_id, period_id),
        FOREIGN KEY (part_id) REFERENCES part(part_id),
        FOREIGN KEY (period_id) REFERENCES period(period_id)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS part (
        part_id INTEGER PRIMARY KEY,
        part_name TEXT,
        lead_time INTEGER,
        initial_inventory INTEGER,
        lot_size INTEGER
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS period (
        period_id INTEGER PRIMARY KEY,
        period_name TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS bom (
        part_id INTEGER,
        component_id INTEGER,
        multiplier INTEGER,
        level INTEGER,
        PRIMARY KEY (part_id, component_id),
        FOREIGN KEY (part_id) REFERENCES part(part_id),
        FOREIGN KEY (component_id) REFERENCES part(part_id)
    )
''')

# Commit the changes 
db.commit()

# Inserting Period data
period_data = [
    (0, 'Week 0'),
    (1, 'Week 1'),
    (2, 'Week 2'),
    (3, 'Week 3'),
    (4, 'Week 4'),
    (5, 'Week 5'),
    (6, 'Week 6'),
    (7, 'Week 7'),
]

# Ensure period_id values are unique
cursor.executemany("INSERT INTO period (period_id, period_name) VALUES (?, ?)", period_data)

# Commit the changes
db.commit()


# Inserting part data
insert_items_table = "INSERT INTO part (part_id, part_name, lead_time, lot_size, initial_inventory) VALUES (?, ?, ?, ?, ?)"
cursor.execute(insert_items_table, (1, 'A', 2, 30, 20))
cursor.execute(insert_items_table, (2, 'B', 2, 50, 40))
cursor.execute(insert_items_table, (3, 'C', 2, 60, 50))

db.commit()

# Inserting bom data
insert_bom_table = "INSERT INTO bom (part_id, component_id, multiplier, level) VALUES (?, ?, ?, ?)"
cursor.execute(insert_bom_table, (1, 2, 1, 0))
cursor.execute(insert_bom_table, (1, 3, 2, 1))
cursor.execute(insert_bom_table, (2, 3, 1, 1))

db.commit()

# Inserting mrp data
insert_mrp_table = "INSERT INTO mrp (part_id, period_id, gross_requirement, scheduled_receipt) VALUES (?, ?, ?, ?)"
Scheduled_receipts_1 = [(1, 0, 0, 0), (1, 1, 100, 0), (1, 2, 50, 60), (1, 3, 90, 0), (1, 4, 30, 0), (1, 5, 10, 0), (1, 6, 100, 0), (1, 7, 20, 0)]
cursor.executemany(insert_mrp_table, Scheduled_receipts_1)
Scheduled_receipts_2 = [(2, 0, 0, 0), (2, 1, 0, 0), (2, 2, 0, 50), (2, 3, 0, 0), (2, 4, 0, 0), (2, 5, 0, 0), (2, 6, 0, 0), (2, 7, 0, 0)]
cursor.executemany(insert_mrp_table, Scheduled_receipts_2)
Scheduled_receipts_3 = [(3, 0, 0, 0), (3, 1, 0, 0), (3, 2, 0, 0), (3, 3, 0, 0), (3, 4, 0, 0), (3, 5, 0, 60), (3, 6, 0, 0), (3, 7, 0, 60)]
cursor.executemany(insert_mrp_table, Scheduled_receipts_3)

db.commit()


# In[6]:


# MRP calculations
create_procedure = '''
CREATE TABLE temporary_table AS
    SELECT 
        mrp1.part_id,
        mrp1.period_id,
        mrp1.gross_requirement,
        CASE 
            WHEN mrp1.period_id=1 AND p.initial_inventory >= (mrp1.gross_requirement - mrp1.scheduled_receipt) THEN 0
            WHEN mrp1.period_id=1 AND p.initial_inventory < (mrp1.gross_requirement - mrp1.scheduled_receipt) THEN (mrp1.gross_requirement - mrp1.scheduled_receipt) - p.initial_inventory
            WHEN x >= (mrp1.gross_requirement - mrp1.scheduled_receipt) THEN 0
            ELSE (mrp1.gross_requirement - mrp1.scheduled_receipt) - x
        END AS net_requirement,
        CASE 
            WHEN mrp1.net_requirement = 0 THEN 0
            ELSE (CAST( mrp1.net_requirement / p.lot_size AS int ) + ( mrp1.net_requirement / p.lot_size > cast ( mrp1.net_requirement / p.lot_size AS int ))) * p.lot_size
        END AS planned_order_receipt,
        mrp1.y AS planned_order_release,
        CASE 
            WHEN mrp1.period_id = 1 AND p.initial_inventory >= (mrp1.gross_requirement - mrp1.scheduled_receipt) THEN p.initial_inventory - (mrp1.gross_requirement - mrp1.scheduled_receipt)
            WHEN mrp1.period_id = 1 AND p.initial_inventory < (mrp1.gross_requirement - mrp1.scheduled_receipt) THEN 0
            WHEN x >= (mrp1.gross_requirement - mrp1.scheduled_receipt) THEN x - (mrp1.gross_requirement - mrp1.scheduled_receipt) 
            ELSE 0
        END AS projected_inventory

    FROM (
        SELECT *, 
            LEAD(mrp1.planned_order_receipt, 2) OVER (PARTITION BY mrp1.part_id ORDER BY mrp1.period_id) AS y,
            LAG(mrp1.projected_inventory, 1) OVER (PARTITION BY mrp1.part_id ORDER BY mrp1.period_id) AS x 
        FROM mrp mrp1
    ) mrp1
    LEFT JOIN bom AS b ON mrp1.part_id = b.part_id 
    LEFT JOIN part AS p ON mrp1.part_id = p.part_id
    WHERE mrp1.period_id <= 7 
    GROUP BY 1, 2, 3, 4, 5, 6, 7;
'''


merge_procedure = ''' 
UPDATE mrp 
SET
    gross_requirement = tt.gross_requirement,
    net_requirement = tt.net_requirement,
    projected_inventory = tt.projected_inventory,
    planned_order_receipt = tt.planned_order_receipt,
    planned_order_release = tt.planned_order_release
FROM temporary_table AS tt
WHERE mrp.part_id = tt.part_id AND mrp.period_id = tt.period_id;

'''

drop_procedure = "DROP TABLE IF EXISTS temporary_table;"

for _ in range(8):
    # Execute create_procedure
    cursor.execute(create_procedure)

    # Execute merge_procedure
    cursor.execute(merge_procedure)

    # Execute drop_procedure
    cursor.execute(drop_procedure)

    # Commit changes after each iteration
    db.commit()
    
# Calculations of gross_requirements for BOM levels
create_temp_table_sql = '''
CREATE TEMPORARY TABLE temp_gross AS
SELECT
    bom.component_id AS part_id,
    mrp.period_id AS period_id,
    SUM(mrp.planned_order_release * bom.multiplier) AS gross_requirement 
FROM  mrp
LEFT JOIN bom ON mrp.part_id = bom.part_id 
GROUP BY 
    bom.component_id, mrp.period_id;
    
'''


update_gross_requirement_sql = '''
UPDATE mrp
SET 
    gross_requirement = tg.gross_requirement
FROM temp_gross AS tg
WHERE mrp.part_id = tg.part_id AND mrp.period_id = tg.period_id;

'''

cursor.execute(create_temp_table_sql)
cursor.execute(update_gross_requirement_sql)
db.commit()

# MRP calculations after BOM Level calculations
for _ in range(8):  # of period times
    cursor.execute(create_procedure)
    cursor.execute(merge_procedure)
    cursor.execute(drop_procedure)
    db.commit()

# Fetch data from the 'mrp' table
select_data_query = 'SELECT * FROM mrp;'
cursor.execute(select_data_query)
results = cursor.fetchall()

# Create a DataFrame from the fetched data
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(results, columns=columns)

# Print the DataFrame
print(df)

cursor.close()
db.close()


# In[ ]:




