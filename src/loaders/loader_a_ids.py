from sqlalchemy import Engine, create_engine, text
import numpy as np

conn_str = "postgresql+psycopg2://root:mypassword@localhost/eticum_app"
engine: Engine = create_engine(conn_str)

connection = engine.connect()

result = connection.execute(text('select id from "activityLog"'))
out = result.all()
converted_list = [item[0] for item in out]
print(converted_list[0:10])
array_ids = np.array(converted_list)
np.save("activityLog_ids.npy", array_ids)
