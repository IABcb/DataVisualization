
from bokeh.plotting import Figure
from bokeh.models import ColumnDataSource, Range1d, Button, Paragraph, TextInput
from bokeh.models import Select, Label, SingleIntervalTicker, LinearAxis
from bokeh.models.layouts import Column
from bokeh.io import curdoc
from bokeh.models.glyphs import VBar, Line
from bokeh.layouts import layout
from random import uniform as uni
from pykafka import KafkaClient
from ast import literal_eval
import time
from datetime import datetime as dt
from time import sleep
from pykafka.common import OffsetType
import threading
import Queue



sources = []
all_column_names = []
counter = 0.0
topic_name = "visualization"
all_names = ["time", "EEUU_Unem", "Spain_Unem", "Japan_Unem", "UK_Unem",
             "EEUU_DJI", "UK_LSE", "Spain_IBEX35", "Japan_N225"]

unem_options = ["EEUU_Unem", "Spain_Unem", "UK_Unem", "Japan_Unem"]
unem_values = ["EEUU_Unem", "Spain_Unem"]

filename = "final_data.csv"

vel_options = ["Slow", "Normal", "Fast"]

source_unem = None

last_date = dt(1999,12,1)
date_stop = dt(2016,11,1)

messages_queue = Queue.Queue()

update_time = 250
sleep_time = 0.0

velocity_options = {"Slow":5.0, "Normal":2.5, "Fast":0.25}

f = open(filename, 'rt')
try:
    for line in f:
        dic_data = {}
        line_list = line.split(",")
        dic_data["Date"] = line_list[0]
        dic_data["EEUU_DJI"] = line_list[1]
        dic_data["UK_LSE"] = line_list[2]
        dic_data["Spain_IBEX35"] = line_list[3]
        dic_data["Japan_N225"] = line_list[4]
        dic_data["EEUU_Unem"] = line_list[5]
        dic_data["UK_Unem"] = line_list[6]
        dic_data["Spain_Unem"] = line_list[7]
        dic_data["Japan_Unem"] = line_list[8][:-1]
        messages_queue.put(str(dic_data))
finally:
    f.close()

# try:
#     client = KafkaClient(hosts="127.0.0.1:9092")
#     topic = client.topics[topic_name]
#     # consumer = topic.get_simple_consumer(auto_offset_reset=OffsetType.LATEST,
#     #                                      reset_offset_on_start=True)
#     consumer = topic.get_simple_consumer()
#
# except:
#     pass


def source_bokeh_kafka(column_names):
    data_dict = {name: [] for name in column_names}
    source = ColumnDataSource(data_dict)
    return source

def multi_plot(figure_info, source):

    fig = Figure(plot_width=figure_info["plot_width"],
                 plot_height=figure_info["plot_height"],
                 title=figure_info["title"], x_axis_type = "datetime")

    fig.extra_y_ranges = {"foo": Range1d(start=0, end=figure_info["max_unemployment"])}
    fig.add_layout(LinearAxis(y_range_name="foo"), 'right')


    for idx in range(1, len(figure_info["names"])):
        legend_name = str(figure_info["legends"][idx-1]) + " "

        if "Unem" not in figure_info["names"][idx]:

            fig.vbar(source=source, x=figure_info["names"][0], top=figure_info["names"][idx],
                     bottom = 0, width = 1000000000, color = figure_info["colors"][idx-1], fill_alpha = 0.2,
                     line_alpha = 0.1, legend = legend_name)

        else:

           fig.line(source=source, x = figure_info["names"][0], y = figure_info["names"][idx],
                     line_width = figure_info["line_widths"][idx-1], alpha = figure_info["alphas"][idx-1],
                     color = figure_info["colors"][idx-1], legend = legend_name, y_range_name="foo")

    fig.legend.location = figure_info["legend_location"]
    fig.xaxis.axis_label = figure_info["xaxis_label"]
    fig.yaxis.axis_label = figure_info["yaxis_label"]
    fig.title.align = figure_info["title_align"]

    return fig


def multiline_plot(figure_info, source):

    fig = Figure(plot_width=figure_info["plot_width"],
                 plot_height=figure_info["plot_height"],
                 title=figure_info["title"], x_axis_type = "datetime")

    for idx in range(1, len(figure_info["names"])):
        legend_name = str(figure_info["legends"][idx-1]) + " "

        fig.line(source=source, x=figure_info["names"][0], y=figure_info["names"][idx],
                 line_width=figure_info["line_widths"][idx - 1], alpha=figure_info["alphas"][idx - 1],
                 color=figure_info["colors"][idx - 1], legend=legend_name)

    fig.legend.location = figure_info["legend_location"]
    fig.xaxis.axis_label = figure_info["xaxis_label"]
    fig.yaxis.axis_label = figure_info["yaxis_label"]
    fig.title.align = figure_info["title_align"]

    return fig


# def multiline_plot(figure_info, source):
#
#     global unem_values
#
#     fig = Figure(plot_width=figure_info["plot_width"],
#                  plot_height=figure_info["plot_height"],
#                  title=figure_info["title"], x_axis_type = "datetime")
#
#     fig.line(source=source, x="time", y=unem_values[0],
#              line_width=2, alpha=0.5,
#              color="blue")
#
#     # fig.line(source=source, x="time", y=unem_values[1],
#     #          line_width=2, alpha=0.5,
#     #          color="red")
#
#     # for idx in range(1, len(figure_info["names"])):
#     #
#     #     legend_name = str(unem_values[idx-1]) + " "
#     #
#     #     # fig.line(source=source, x = figure_info["names"][0], y = figure_info["names"][idx],
#     #     #          line_width = figure_info["line_widths"][idx-1], alpha = figure_info["alphas"][idx-1],
#     #     #          color = figure_info["colors"][idx-1], legend = legend_name)
#     #
#     #     print("Current unem values ", unem_values[idx-1])
#     #
#     #     # fig.line(source=source, x=figure_info["names"][0], y=unem_values[idx-1],
#     #     #          line_width=figure_info["line_widths"][idx - 1], alpha=figure_info["alphas"][idx - 1],
#     #     #          color=figure_info["colors"][idx - 1])
#     #     #
#     #
#     #     fig.line(source=source, x=figure_info["names"][0], y=unem_values[idx-1],
#     #              line_width=figure_info["line_widths"][idx - 1], alpha=figure_info["alphas"][idx - 1],
#     #              color=figure_info["colors"][idx - 1])
#
#
#     fig.legend.location = figure_info["legend_location"]
#     fig.xaxis.axis_label = figure_info["xaxis_label"]
#     fig.yaxis.axis_label = figure_info["yaxis_label"]
#     fig.title.align = figure_info["title_align"]
#
#     return fig


def update_data():

    global sources
    global all_column_names
    global counter
    global consumer
    global last_date
    global source_unem
    global update_time

    sleep(sleep_time)

    # print("Sleeping time ", sleep_time)


    if last_date >= date_stop:

        pass
        # print("getting data 1")

    else:

        # print("getting data 2")

        try:
            # kafka
            # message = consumer.consume()

            # kafka
            # if message is not None:

            if not messages_queue.empty():

                # print("getting data 3")

                # kafka
                # value = message.value

                value = messages_queue.get()

                dict_message = literal_eval(value)

                for source, column_names in zip(sources, all_column_names):
                    data_dict = {name: [dict_message[name]] for name in column_names if name != "time"}

                    print(dict_message["Date"])

                    year, month = dict_message["Date"].split("-")
                    data_dict[column_names[0]] = [dt(int(year), int(month), 1)]

                    source.stream(data_dict, 1000)

                    last_date = dt(int(year), int(month), 1)



                    # source_unem.data["time"]=source.data["time"]
                    # source_unem.data[unem_values[0]] = source.data[unem_values[0]]
                    # source_unem.data[unem_values[1]] = source.data[unem_values[1]]
        except:
            pass

        # print(dir(source))

        # new_column_data_source = source["time"]
        #
        # print(new_column_data_source.column_names)



def main():

    global sources
    global all_column_names
    global counter
    global consumer
    global all_names
    global unem_options
    global unem_values
    global source_unem
    global update_time
    global sleep_time
    global velocity_options

    # def update_select_unem1(attr, old, new):
    #     global unem_values
    #
    #     unem1_value = select_unem1.value
    #     unem_values[0] = unem1_value
    #
    #     print("Changing unem 1 ", unem_values[0])


    def update_select_vel(attr, old, new):
        global sleep_time
        global velocity_options

        speed = select_vel.value

        print("Changing speed to ", speed, velocity_options[speed])

        sleep_time = velocity_options[speed]




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



    figure_info5 = {"names":["time", "EEUU_Unem", "Spain_Unem", "Japan_Unem", "UK_Unem"],
                   "x_name":"time", "line_widths":[2,2,2,2] ,
                    "alphas":[0.85, 0.85, 0.85, 0.85],
                    "colors":["blue", "red", "black", "orange"],
                    "legends":["EEUU_Unem", "Spain_Unem", "Japan_Unem", "UK_Unem"],
                    "y_range":[0, 30],
                    "plot_width":450, "plot_height":350,
                    "title":"Unemployment comparison", "legend_location":"top_left",
                    "xaxis_label":"x", "yaxis_label":"y",
                    "title_align":"center"}


    figure_info6 = {"names":["time", "EEUU_DJI", "Spain_IBEX35", "Japan_N225", "UK_LSE"],
                   "x_name":"time", "line_widths":[2,2,2,2] ,
                    "alphas":[0.85, 0.85, 0.85, 0.85],
                    "colors":["blue", "red", "black", "orange"],
                    "legends":["EEUU_DJI", "Spain_IBEX35", "Japan_N225", "UK_LSE"],
                    "y_range":[0, 30],
                    "plot_width":450, "plot_height":350,
                    "title":"Stock exchange comparison", "legend_location":"top_left",
                    "xaxis_label":"x", "yaxis_label":"y",
                    "title_align":"center"}


    # figure_info5 = {"names":["time", unem_values[0], unem_values[1]],
    #                "x_name":"time", "line_widths":[2,2] ,
    #                 "alphas":[0.85, 0.85],
    #                 "colors":["blue", "red"],
    #                 "plot_width":450, "plot_height":350,
    #                 "title":"UK", "legend_location":"top_left",
    #                 "xaxis_label":"x", "yaxis_label":"y",
    #                 "title_align":"center",
    #                 "max_unemployment":10}


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
    dict_unem = {"time":source_all.data["time"],
                 unem_values[0]: source_all.data[unem_values[0]],
                 unem_values[0]: source_all.data[unem_values[0]]}

    source_unem = ColumnDataSource(dict_unem)




    fig1 = multi_plot(figure_info1 ,source_all)
    fig2 = multi_plot(figure_info2, source_all)
    fig3 = multi_plot(figure_info3, source_all)
    fig4 = multi_plot(figure_info4, source_all)
    fig5 = multiline_plot(figure_info5, source_all)
    fig6 = multiline_plot(figure_info6, source_all)

    select_vel = Select(value='Slow', options=vel_options)
    select_vel.on_change('value', update_select_vel)


    def update_buttom_increase():
        global sleep_time
        sleep_time -= 0.15
        print("increasing velocity")


    def update_buttom_decrease():
        global sleep_time
        sleep_time += 0.15
        print("increasing velocity")



    buttom_increase = Button(label="Increase velocity")
    buttom_increase.on_click(update_buttom_increase)

    buttom_decrease = Button(label="Decrease velocity")
    buttom_decrease.on_click(update_buttom_decrease)


    sources.append(source_all)
    all_column_names.append(all_names)

    # select_unem1.on_change('value', update_select_unem1)

    curdoc().add_periodic_callback(update_data, update_time)

    curdoc().add_root(layout([[fig1, fig2, fig5],
                              [fig3, fig4, fig6]]))




main()

