# data alarmlist
ALARMLIST_PATH = '/data/data_alarmlist' #'/data/log_alarmlist' ,'D:/data/data_alarmlist'
ALARMLIST_TABLE = 'data_alarmlist'
ALARMLIST_TABLE_COLUMNS ='''
                registered_at datetime,
                topic varchar(50),
                occurred varchar(50),
                restored varchar(50),
                time_diff int,
                mc_no varchar(50),'''

ALARMLIST_TABLE_LOG = 'log_alarmlist'
ALARMLIST_TABLE_COLUMNS_LOG ='''
            registered_at datetime,
			status varchar(50),
            file_name varchar(50),
            process varchar(50),
            message varchar(50),
            error varchar(MAX),
            '''

# data mc_status
MC_STATUS_PATH = '/data/data_mc_status' #'/data/log_mc_status' ,'D:/data/data_mc_status'
MC_STATUS_TABLE = 'data_mc_status'
MC_STATUS_TABLE_COLUMNS ='''
            registered_at datetime,
			occurred datetime,
			mc_status varchar(50),
            mc_no varchar(50),
            '''

MC_STATUS_TABLE_LOG = 'log_mc_status'
MC_STATUS_TABLE_COLUMNS_LOG ='''
            registered_at datetime,
			status varchar(50),
            file_name varchar(50),
            process varchar(50),
            message varchar(50),
            error varchar(MAX),
            '''

#LOG status
STATUS_OK = 'ok'
STATUS_ERROR = 'error'
STATUS_INFO = 'info'