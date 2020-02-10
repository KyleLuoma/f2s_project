# -*- coding: utf-8 -*-
"""
Created on Mon Feb 9 14:22:39 2020
@author: LuomaKR
Module to process data files loaded for F2S matching
"""

import pandas as pd

"""Process the AOS spaces billet export Pandas DF(s)"""
def process_aos_billet_export(aos_billet_export):
    aos_billet_export["stage_matched"] = 0
    return aos_billet_export

