
from bokeh.plotting import Figure
from bokeh.models import ColumnDataSource, Range1d, Button, Paragraph
from bokeh.models import Select, Label, SingleIntervalTicker, LinearAxis
from bokeh.io import curdoc
from bokeh.models.glyphs import VBar, Line
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

def multil_plot(figure_info, source):

    fig = Figure(plot_width=figure_info["plot_width"],
                 plot_height=figure_info["plot_height"],
                 title=figure_info["title"], x_axis_type = "datetime")

    fig.extra_y_ranges = {"foo": Range1d(start=0, end=figure_info["max_unemployment"])}
    fig.add_layout(LinearAxis(y_range_name="foo"), 'right')


    for idx in range(1, len(figure_info["names"])):
        legend_name = str(figure_info["legends"][idx-1]) + " "

        if "Unem" not in figure_info["names"][idx]:

            # glyph_bar = VBar(x=figure_info["names"][0], top=figure_info["names"][idx],
            #          bottom=0, width=1000000000, fill_color="#b3de69", line_width = 0,
            #          line_alpha = 1.0)
            #
            # fig.add_glyph(source, glyph_bar)

            fig.vbar(source=source, x=figure_info["names"][0], top=figure_info["names"][idx],
                     bottom = 0, width = 1000000000, color = figure_info["colors"][idx-1], fill_alpha = 0.2,
                     line_alpha = 0.1, legend = legend_name)
        #2592000000
        #1000000000

        else:

            # glyph = Line(x = figure_info["names"][0], y = figure_info["names"][idx],
            #          line_width = figure_info["line_widths"][idx-1], line_alpha = figure_info["alphas"][idx-1],
            #          line_color = figure_info["colors"][idx-1], y_range_name="foo")
            #
            # fig.add_glyph(source, glyph)


           fig.line(source=source, x = figure_info["names"][0], y = figure_info["names"][idx],
                     line_width = figure_info["line_widths"][idx-1], alpha = figure_info["alphas"][idx-1],
                     color = figure_info["colors"][idx-1], legend = legend_name, y_range_name="foo")


            # glyph = Line(x = figure_info["names"][0], y = figure_info["names"][idx],
            #          line_width = figure_info["line_widths"][idx-1], line_alpha = figure_info["alphas"][idx-1],
            #          line_color = figure_info["colors"][idx-1])
            #
            # fig.add_glyph(source, glyph)

            # fig.line(source=source, x = figure_info["names"][0], y = figure_info["names"][idx],
            #          line_width = figure_info["line_widths"][idx-1], alpha = figure_info["alphas"][idx-1],
            #          color = figure_info["colors"][idx-1], legend = legend_name)

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

    figure_info1 = {"names":["time", "EEUU_Unem", "EEUU_DJI"],
                   "x_name":"time", "line_widths":[2,2,2,2] ,
                    "alphas":[0.85, 0.85, 0.85, 0.85],
                    "colors":["blue", "red", "black", "orange"],
                    "legends":["Unem", "DJI"],
                    "y_range":[0, 30],
                    "plot_width":450, "plot_height":350,
                    "title":"EEUU", "legend_location":"top_left",
                    "xaxis_label":"x", "yaxis_label":"y",
                    "title_align":"center",
                    "max_unemployment":15}


    figure_info2 = {"names":["time", "Spain_Unem", "Spain_IBEX35"],
                   "x_name":"time", "line_widths":[2,2,2,2] ,
                    "alphas":[0.85, 0.85, 0.85, 0.85],
                    "colors":["blue", "red", "black", "orange"],
                    "legends":["Unem", "IBEX35"],
                    "y_range":[0, 30],
                    "plot_width":450, "plot_height":350,
                    "title":"Spain", "legend_location":"top_left",
                    "xaxis_label":"x", "yaxis_label":"y",
                    "title_align":"center",
                    "max_unemployment":30}


    figure_info3 = {"names":["time", "Japan_Unem", "Japan_N225"],
                   "x_name":"time", "line_widths":[2,2,2,2] ,
                    "alphas":[0.85, 0.85, 0.85, 0.85],
                    "colors":["blue", "red", "black", "orange"],
                    "legends":["Unem", "N255"],
                    "y_range":[0, 30],
                    "plot_width":450, "plot_height":350,
                    "title":"Japan", "legend_location":"top_left",
                    "xaxis_label":"x", "yaxis_label":"y",
                    "title_align":"center",
                    "max_unemployment":8}


    figure_info4 = {"names":["time", "UK_Unem", "UK_LSE"],
                   "x_name":"time", "line_widths":[2,2,2,2] ,
                    "alphas":[0.85, 0.85, 0.85, 0.85],
                    "colors":["blue", "red", "black", "orange"],
                    "legends":["Unem", "LSE"],
                    "y_range":[0, 30],
                    "plot_width":450, "plot_height":350,
                    "title":"UK", "legend_location":"top_left",
                    "xaxis_label":"x", "yaxis_label":"y",
                    "title_align":"center",
                    "max_unemployment":10}


    # figure_info1 = {"names":["time", "EEUU_Unem", "Spain_Unem", "Japan_Unem", "UK_Unem"],
    #                "x_name":"time", "line_widths":[2,2,2,2] ,
    #                 "alphas":[0.85, 0.85, 0.85, 0.85],
    #                 "colors":["blue", "red", "black", "orange"],
    #                 "legends":["EEUU", "Spain", "Japan", "UK"],
    #                 "y_range":[0, 30],
    #                 "plot_width":450, "plot_height":350,
    #                 "title":"Title for figure", "legend_location":"bottom_left",
    #                 "xaxis_label":"x", "yaxis_label":"y",
    #                 "title_align":"center"}



    # figure_info2 = {"names":["time", "EEUU_DJI", "Spain_IBEX35", "Japan_N225", "UK_LSE"],
    #                "x_name":"time", "line_widths":[2,2,2,2] ,
    #                 "alphas":[0.85, 0.85, 0.85, 0.85],
    #                 "colors":["blue", "red", "black", "orange"],
    #                 "legends":["DJI", "IBEX35", "N225", "LSE"],
    #                 "y_range":[0, 22000],
    #                 "plot_width":450, "plot_height":350,
    #                 "title":"Title for figure", "legend_location":"bottom_left",
    #                 "xaxis_label":"x", "yaxis_label":"y",
    #                 "title_align":"center"}


    source_all = source_bokeh_kafka(all_names)

    fig1 = multil_plot(figure_info1 ,source_all)
    fig2 = multil_plot(figure_info2, source_all)
    fig3 = multil_plot(figure_info3, source_all)
    fig4 = multil_plot(figure_info4, source_all)

    sources.append(source_all)
    all_column_names.append(all_names)
    curdoc().add_periodic_callback(update_data, update_time)

    curdoc().add_root(layout([[fig1, fig2],[fig3, fig4]]))

main()

