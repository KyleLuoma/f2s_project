# -*- coding: utf-8 -*-
import bokeh
from bokeh.plotting import output_file, figure, show


def match_phases_pivot(faces_matches, cmd = ["ALL"], width = 800, height = 400):
    if(cmd[0] == "ALL"):
        cmd = faces_matches[["STRUC_CMD_CD"]].groupby("STRUC_CMD_CD").count().index.to_list()
    
    matches = faces_matches.where(faces_matches.STRUC_CMD_CD.isin(cmd)).dropna(how = "all")
    
    output_file("../output/match_phases_pivot.html")
    plt = figure(plot_width = width, plot_height = height)
    plt.vbar(
        x = matches[["stage_matched", "SSN_MASK"]].groupby("stage_matched").count().index,
        width = 0.5,
        bottom = 0,
        top = matches[["stage_matched", "SSN_MASK"]].groupby("stage_matched").count().SSN_MASK
    )
    show(plt)