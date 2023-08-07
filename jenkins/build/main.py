import utils.constant as constant
import os

from utils.csv_to_db import ALARMLIST,MC_STATUS
from dotenv import load_dotenv

load_dotenv()

# alarmlist = ALARMLIST(
#         path=constant.ALARMLIST_PATH,
#         server=os.getenv('SERVER'),
#         database=os.getenv('DATABASE'),
#         user_login=os.getenv('USER_LOGIN'),
#         password=os.getenv('PASSWORD'),
#         table=constant.ALARMLIST_TABLE,
#         table_columns=constant.ALARMLIST_TABLE_COLUMNS,
#         table_log=constant.ALARMLIST_TABLE_LOG,
#         table_columns_log=constant.ALARMLIST_TABLE_COLUMNS_LOG,
#         line_notify_token=os.getenv('LINE_NOTIFY_TOKEN'),
#     )

# alarmlist.run()

mc_status = MC_STATUS(
        path=constant.MC_STATUS_PATH,
        server=os.getenv('SERVER'),
        database=os.getenv('DATABASE'),
        user_login=os.getenv('USER_LOGIN'),
        password=os.getenv('PASSWORD'),
        table=constant.MC_STATUS_TABLE,
        table_columns=constant.MC_STATUS_TABLE_COLUMNS,
        table_log=constant.MC_STATUS_TABLE_LOG,
        table_columns_log=constant.MC_STATUS_TABLE_COLUMNS_LOG,
        line_notify_token=os.getenv('LINE_NOTIFY_TOKEN'),
    )

mc_status.run()