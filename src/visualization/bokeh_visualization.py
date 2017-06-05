

from bokeh.models import ColumnDataSource, Button, Paragraph, Select, Label, SingleIntervalTicker, LinearAxis


def multilinear_plot(names):
    data = dict(())
    source = ColumnDataSource(dict(time=[], youtube=[], youtube=[]))