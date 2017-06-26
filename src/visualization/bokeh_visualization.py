
from bokeh.plotting import Figure
from bokeh.models import ColumnDataSource, Range1d, Button, Paragraph
from bokeh.models import Select, Label, SingleIntervalTicker, LinearAxis
from bokeh.io import curdoc
from bokeh.layouts import layout
from random import uniform as uni
from pykafka import KafkaClient
from ast import literal_eval
from datetime import datetime as dt


sources = []
all_column_names = []
counter = 0.0
topic_name = "visualization"
all_names = ["time", "EEUU_Unem", "Spain_Unem", "Japan_Unem", "UK_Unem",
             "EEUU_DJI", "UK_LSE", "Spain_IBEX35", "Japan_N225"]

client = KafkaClient(hosts="127.0.0.1:9092")
topic = client.topics[topic_name]
consumer = topic.get_simple_consumer()

def source_bokeh_kafka(column_names):
    data_dict = {name: [] for name in column_names}
    data_dict["time"] = [dt(1999, 12, 1)]
    source = ColumnDataSource(data_dict)
    return source

def multilinear_plot(figure_info, source):

    fig = Figure(y_range = Range1d(figure_info["y_range"][0], figure_info["y_range"][1]),
                 plot_width=figure_info["plot_width"],
                 plot_height=figure_info["plot_height"],
                 title=figure_info["title"], x_axis_type = "datetime")


    for idx in range(1, len(figure_info["names"])):
        legend_name = str(figure_info["legends"][idx-1]) + " "
        fig.line(source=source, x = figure_info["names"][0], y = figure_info["names"][idx],
                 line_width = figure_info["line_widths"][idx-1], alpha = figure_info["alphas"][idx-1],
                 color = figure_info["colors"][idx-1], legend = legend_name)

    fig.legend.location = figure_info["legend_location"]
    fig.xaxis.axis_label = figure_info["xaxis_label"]
    fig.yaxis.axis_label = figure_info["yaxis_label"]
    fig.title.align = figure_info["title_align"]

    return fig

def update_data():

    global sources
    global all_column_names
    global counter
    global consumer

    message = consumer.consume()
    if message is not None:
        value = message.value
        dict_message = literal_eval(value)

        for source, column_names in zip(sources, all_column_names):
            data_dict = {name: [dict_message[name]] for name in column_names if name != "time"}

            print(dict_message["Date"])

            year, month = dict_message["Date"].split("-")
            data_dict[column_names[0]] = [dt(int(year), int(month), 1)]

            source.stream(data_dict, 1000)

def main():

    global sources
    global all_column_names
    global counter
    global consumer
    global all_names

    update_time = 100

    figure_info1 = {"names":["time", "EEUU_Unem", "Spain_Unem", "Japan_Unem", "UK_Unem"],
                   "x_name":"time", "line_widths":[2,2,2,2] ,
                    "alphas":[0.85, 0.85, 0.85, 0.85],
                    "colors":["blue", "red", "black", "orange"],
                    "legends":["EEUU", "Spain", "Japan", "UK"],
                    "y_range":[0, 30],
                    "plot_width":450, "plot_height":350,
                    "title":"Title for figure", "legend_location":"bottom_left",
                    "xaxis_label":"x", "yaxis_label":"y",
                    "title_align":"center"}


    figure_info2 = {"names":["time", "EEUU_DJI", "Spain_IBEX35", "Japan_N225", "UK_LSE"],
                   "x_name":"time", "line_widths":[2,2,2,2] ,
                    "alphas":[0.85, 0.85, 0.85, 0.85],
                    "colors":["blue", "red", "black", "orange"],
                    "legends":["DJI", "IBEX35", "N225", "LSE"],
                    "y_range":[0, 22000],
                    "plot_width":450, "plot_height":350,
                    "title":"Title for figure", "legend_location":"bottom_left",
                    "xaxis_label":"x", "yaxis_label":"y",
                    "title_align":"center"}


    source_all = source_bokeh_kafka(all_names)

    fig1 = multilinear_plot(figure_info1 ,source_all)
    fig2 = multilinear_plot(figure_info2, source_all)

    sources.append(source_all)
    all_column_names.append(all_names)
    curdoc().add_periodic_callback(update_data, update_time)

    curdoc().add_root(layout([[fig1, fig2]]))

main()

