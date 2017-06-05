
from bokeh.plotting import Figure
from bokeh.models import ColumnDataSource, Range1d, Button, Paragraph, Select, Label, SingleIntervalTicker, LinearAxis
from bokeh.io import curdoc
from bokeh.layouts import layout
from random import uniform as uni

def multilinear_plot(figure_info):

    data_dict = {name:[uni(-1.5, 1.5),uni(-1.5, 1.5),uni(-1.5, 1.5),uni(-1.5, 1.5)] for name in figure_info["names"]}
    data_dict[figure_info["x_name"]] = [0,1,2,3]

    source = ColumnDataSource(data_dict)
    fig = Figure(x_range = Range1d(figure_info["x_range"][0], figure_info["x_range"][1]),
                 y_range = Range1d(figure_info["y_range"][0], figure_info["y_range"][1]),
                 plot_width=figure_info["plot_width"],
                 plot_height=figure_info["plot_height"],
                 title=figure_info["title"])

    for idx in range(len(figure_info["names"])):
        fig.line(source=source, x = figure_info["x_name"], y = figure_info["names"][idx],
                 line_width = figure_info["line_widths"][idx], alpha = figure_info["alphas"][idx],
                 color = figure_info["colors"][idx], legend = figure_info["legends"][idx])

    fig.legend.location = figure_info["legend_location"]
    fig.xaxis.axis_label = figure_info["xaxis_label"]
    fig.yaxis.axis_label = figure_info["yaxis_label"]
    fig.title.align = figure_info["title_align"]

    return fig


def main():


    figure_info1 = {"names":["EEUU", "Spain", "Japan", "UK"],
                   "x_name":"time", "line_widths":[2,2,2,2] ,
                    "alphas":[0.85, 0.85, 0.85, 0.85],
                    "colors":["blue", "red", "black", "orange"],
                    "legends":["EEUU", "Spain", "Japan", "UK"],
                    "x_range":[0, 4], "y_range":[-1.5, 1.5],
                    "plot_width":550, "plot_height":440,
                    "title":"Title", "legend_location":"bottom_right",
                    "xaxis_label":"x", "yaxis_label":"y",
                    "title_align":"center"}

    fig1 = multilinear_plot(figure_info1)
    curdoc().add_root(layout([[fig1]]))

main()

