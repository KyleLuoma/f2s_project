# -*- coding: utf-8 -*-

import time as tm

def get_file_timestamp():
    t = tm.localtime()
    return (
            str(t.tm_year) + "-" +
            str(t.tm_mon)  + "-" +
            str(t.tm_mday) + "-" +
            str(t.tm_hour) + "-" +
            str(t.tm_min)  + "-" +
            str(t.tm_sec)
            )

def get_local_time_as_string():
    return tm.ctime()