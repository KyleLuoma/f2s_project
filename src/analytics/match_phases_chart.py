# -*- coding: utf-8 -*-
import bokeh
from bokeh.plotting import output_file, figure, show


def match_phases_pivot(faces_matches, cmd = ["ALL"], width = 800, height = 400):
    name = "Faces to Space Phase Matches for: "
    if(cmd[0] == "ALL"):
        name = name + "All Commands"
        cmd = faces_matches[["STRUC_CMD_CD"]].groupby("STRUC_CMD_CD").count().index.to_list()
    
    matches = faces_matches.where(faces_matches.STRUC_CMD_CD.isin(cmd)).dropna(how = "all")
    stages = matches[["stage_matched", "SSN_MASK"]].groupby("stage_matched").count().index
    phase_values = matches[["stage_matched", "SSN_MASK"]].groupby("stage_matched").count().SSN_MASK
    multi_line = [[matches.shape[0]] * phase_values.shape[0],
                  accumulate_phase_counts(phase_values).to_list(),
                  calculate_remaining_faces(phase_values).to_list()]
    
    output_file("../output/match_phases_pivot.html")
    plt = figure(
        plot_width = width, 
        plot_height = height,
        y_axis_label = "Count of Faces",
        y_axis_type = "linear",
        x_axis_label = "Match Phases",
        x_axis_type = "linear"
    )
    plt.varea(x = stages, y1 = multi_line[1], y2 = [0] * len(stages), color = "gray")
    #plt.varea(x = stages, y1 = multi_line[2], y2 = [0] * len(stages), color = "black")
    #Stage matches bar chart:
    plt.multi_line(
        xs = [stages, stages, stages],
        ys = multi_line,
        color = ["blue", "black", "green"],
        line_width = ["2", "2", "3"]
    )
    plt.vbar(
        x = stages[1:len(stages)],
        width = 0.5,
        bottom = 0,
        top = phase_values[1:len(stages)]
    )
    show(plt)
    
    
def accumulate_phase_counts(phase_values):
    accumulation = phase_values.copy()
    for i in range(2, len(phase_values)):
        accumulation.iloc[i] = accumulation.iloc[i - 1] + phase_values.iloc[i]
    accumulation.iloc[0] = 0
    return accumulation

def calculate_remaining_faces(phase_values):
    remaining = phase_values.copy()
    remaining.iloc[0] = phase_values.sum()
    for i in range(1, len(phase_values)):
        remaining.iloc[i] = remaining.iloc[i -1] - phase_values.iloc[i]
    return remaining
    
    
